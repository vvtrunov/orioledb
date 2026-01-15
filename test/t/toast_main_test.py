#!/usr/bin/env python3
# coding: utf-8

import base64
import os
from .base_test import BaseTest
from .base_test import generate_string as gen_str


class ToastMainTest(BaseTest):

    def test_toast_main_single_column_basic(self):
        """
        Test basic TOAST functionality with a single STORAGE MAIN column.
        This tests that compressible data in a MAIN column is handled correctly.
        """
        node = self.node
        node.start()

        node.safe_psql(
            'postgres',
            """
            CREATE EXTENSION IF NOT EXISTS orioledb;
            CREATE TABLE o_test_main (
                id integer PRIMARY KEY,
                m text
            ) USING orioledb;
            ALTER TABLE o_test_main ALTER COLUMN m SET STORAGE MAIN;
            """
        )

        # Insert a large compressible value
        large = gen_str(5000, 1)
        node.safe_psql('postgres', "INSERT INTO o_test_main VALUES (1, '%s');" % large)

        # Verify data is retrievable and has expected length
        res = node.execute("SELECT length(m) FROM o_test_main WHERE id = 1;")
        self.assertEqual(res[0][0], 5000)

        # Verify the actual data matches
        res = node.execute("SELECT m FROM o_test_main WHERE id = 1;")
        self.assertEqual(res[0][0], large)

        node.stop()

    def test_toast_main_incompressible_data(self):
        """
        Test STORAGE MAIN with incompressible data (random bytes).
        This should trigger compression attempt that fails, then fall back
        to out-of-line toasting if the data is large enough.
        """
        node = self.node
        node.start()

        node.safe_psql(
            'postgres',
            """
            CREATE EXTENSION IF NOT EXISTS orioledb;
            CREATE TABLE o_test_incompressible (
                id integer PRIMARY KEY,
                data bytea
            ) USING orioledb;
            ALTER TABLE o_test_incompressible ALTER COLUMN data SET STORAGE MAIN;
            """
        )

        # Generate incompressible data (random bytes, base64 encoded for transfer)
        # Use enough data to exceed reasonable compression thresholds
        random_bytes = os.urandom(8000)
        encoded_data = base64.b64encode(random_bytes).decode('ascii')

        node.safe_psql(
            'postgres',
            "INSERT INTO o_test_incompressible VALUES (1, decode('%s', 'base64'));" % encoded_data
        )

        # Verify data integrity
        res = node.execute("SELECT length(data) FROM o_test_incompressible WHERE id = 1;")
        self.assertEqual(res[0][0], 8000)

        # Verify actual data matches
        res = node.execute("SELECT encode(data, 'base64') FROM o_test_incompressible WHERE id = 1;")
        retrieved_encoded = res[0][0].replace('\n', '')
        self.assertEqual(retrieved_encoded, encoded_data)

        node.stop()

    def test_toast_main_order_verification(self):
        """
        Stronger test to verify largest-first toasting order.

        Creates a scenario where we can definitively verify order:
        - 4 MAIN columns of significantly different sizes
        - Data large enough to require toasting multiple columns
        - Verify that largest columns are toasted, not smallest ones
        """
        node = self.node
        node.start()

        node.safe_psql(
            'postgres',
            """
            CREATE EXTENSION IF NOT EXISTS orioledb;
            CREATE TABLE o_test_toast_order (
                id integer PRIMARY KEY,
                tiny text,
                small text,
                medium text,
                large text
            ) USING orioledb;
            ALTER TABLE o_test_toast_order ALTER COLUMN tiny SET STORAGE MAIN;
            ALTER TABLE o_test_toast_order ALTER COLUMN small SET STORAGE MAIN;
            ALTER TABLE o_test_toast_order ALTER COLUMN medium SET STORAGE MAIN;
            ALTER TABLE o_test_toast_order ALTER COLUMN large SET STORAGE MAIN;
            """
        )

        # Create very different sizes to make the test deterministic
        # Using incompressible data (base64 of random bytes)
        tiny_data = base64.b64encode(os.urandom(500)).decode('ascii')    # ~667 bytes encoded
        small_data = base64.b64encode(os.urandom(2000)).decode('ascii')  # ~2667 bytes
        medium_data = base64.b64encode(os.urandom(4000)).decode('ascii') # ~5333 bytes
        large_data = base64.b64encode(os.urandom(6000)).decode('ascii')  # ~8000 bytes

        node.safe_psql(
            'postgres',
            """
            INSERT INTO o_test_toast_order VALUES (
                1,
                '%s',
                '%s',
                '%s',
                '%s'
            );
            """ % (tiny_data, small_data, medium_data, large_data)
        )

        # Check which columns were toasted out-of-line using pg_column_toast_chunk_id
        res = node.execute(
            """
            SELECT
                pg_column_toast_chunk_id(tiny) IS NOT NULL as tiny_toasted,
                pg_column_toast_chunk_id(small) IS NOT NULL as small_toasted,
                pg_column_toast_chunk_id(medium) IS NOT NULL as medium_toasted,
                pg_column_toast_chunk_id(large) IS NOT NULL as large_toasted
            FROM o_test_toast_order WHERE id = 1;
            """
        )
        tiny_toasted = res[0][0]
        small_toasted = res[0][1]
        medium_toasted = res[0][2]
        large_toasted = res[0][3]

        # The invariant we're testing: if a smaller column is toasted,
        # all larger columns must also be toasted (largest-first ordering)
        if tiny_toasted and not small_toasted:
            self.fail(
                "Toasting order violated: tiny toasted out-of-line "
                "but small not toasted. Should toast largest first!"
            )

        if tiny_toasted and not medium_toasted:
            self.fail(
                "Toasting order violated: tiny toasted out-of-line "
                "but medium not toasted. Should toast largest first!"
            )

        if tiny_toasted and not large_toasted:
            self.fail(
                "Toasting order violated: tiny toasted out-of-line "
                "but large not toasted. Should toast largest first!"
            )

        if small_toasted and not medium_toasted:
            self.fail(
                "Toasting order violated: small toasted out-of-line "
                "but medium not toasted. Should toast largest first!"
            )

        if small_toasted and not large_toasted:
            self.fail(
                "Toasting order violated: small toasted out-of-line "
                "but large not toasted. Should toast largest first!"
            )

        if medium_toasted and not large_toasted:
            self.fail(
                "Toasting order violated: medium toasted out-of-line "
                "but large not toasted. Should toast largest first!"
            )

        # Verify data integrity - all columns should be retrievable correctly
        res = node.execute(
            """
            SELECT tiny, small, medium, large
            FROM o_test_toast_order WHERE id = 1;
            """
        )
        self.assertEqual(res[0][0], tiny_data)
        self.assertEqual(res[0][1], small_data)
        self.assertEqual(res[0][2], medium_data)
        self.assertEqual(res[0][3], large_data)

        node.stop()

    def test_toast_main_with_extended_storage_mix(self):
        """
        Test behavior when mixing STORAGE MAIN and STORAGE EXTENDED columns.
        EXTENDED columns should be toasted before MAIN columns (MAIN is last resort).
        """
        node = self.node
        node.start()

        node.safe_psql(
            'postgres',
            """
            CREATE EXTENSION IF NOT EXISTS orioledb;
            CREATE TABLE o_test_mixed_storage (
                id integer PRIMARY KEY,
                main_col text,
                extended_col text
            ) USING orioledb;
            ALTER TABLE o_test_mixed_storage ALTER COLUMN main_col SET STORAGE MAIN;
            ALTER TABLE o_test_mixed_storage ALTER COLUMN extended_col SET STORAGE EXTENDED;
            """
        )

        # Both columns have large incompressible data
        main_data = base64.b64encode(os.urandom(5000)).decode('ascii')
        extended_data = base64.b64encode(os.urandom(5000)).decode('ascii')

        node.safe_psql(
            'postgres',
            """
            INSERT INTO o_test_mixed_storage VALUES (
                1,
                '%s',
                '%s'
            );
            """ % (main_data, extended_data)
        )

        # Verify data integrity
        res = node.execute(
            """
            SELECT length(main_col), length(extended_col)
            FROM o_test_mixed_storage WHERE id = 1;
            """
        )
        self.assertEqual(res[0][0], len(main_data))
        self.assertEqual(res[0][1], len(extended_data))

        res = node.execute(
            """
            SELECT main_col, extended_col
            FROM o_test_mixed_storage WHERE id = 1;
            """
        )
        self.assertEqual(res[0][0], main_data)
        self.assertEqual(res[0][1], extended_data)

        # Check which columns were toasted out-of-line to verify EXTENDED is preferred over MAIN
        res = node.execute(
            """
            SELECT
                pg_column_toast_chunk_id(main_col) IS NOT NULL as main_toasted,
                pg_column_toast_chunk_id(extended_col) IS NOT NULL as extended_toasted
            FROM o_test_mixed_storage WHERE id = 1;
            """
        )
        main_toasted = res[0][0]
        extended_toasted = res[0][1]

        # If MAIN is toasted, EXTENDED should definitely be toasted (EXTENDED has higher priority)
        if main_toasted and not extended_toasted:
            self.fail(
                "Storage policy violated: MAIN column toasted out-of-line "
                "but EXTENDED column not toasted. "
                "EXTENDED should be toasted before MAIN!"
            )

        # At least one column should be toasted given the data sizes
        self.assertTrue(
            main_toasted or extended_toasted,
            "Expected at least one column to be toasted out-of-line with large incompressible data"
        )

        node.stop()

    def test_toast_main_update_scenario(self):
        """
        Test updating rows with STORAGE MAIN columns to ensure
        re-toasting logic works correctly.
        """
        node = self.node
        node.start()

        node.safe_psql(
            'postgres',
            """
            CREATE EXTENSION IF NOT EXISTS orioledb;
            CREATE TABLE o_test_main_update (
                id integer PRIMARY KEY,
                m text
            ) USING orioledb;
            ALTER TABLE o_test_main_update ALTER COLUMN m SET STORAGE MAIN;
            """
        )

        # Insert small value
        small = gen_str(100, 1)
        node.safe_psql('postgres', "INSERT INTO o_test_main_update VALUES (1, '%s');" % small)

        # Verify small value is not toasted out-of-line
        res = node.execute("SELECT pg_column_toast_chunk_id(m) FROM o_test_main_update WHERE id = 1;")
        small_chunk_id = res[0][0]
        # Small value should not be toasted out-of-line
        self.assertIsNone(
            small_chunk_id,
            f"Small value should not be toasted out-of-line, but chunk_id={small_chunk_id}"
        )

        # Update to large incompressible value
        large = base64.b64encode(os.urandom(8000)).decode('ascii')
        node.safe_psql('postgres', "UPDATE o_test_main_update SET m = '%s' WHERE id = 1;" % large)

        # Verify updated data length
        res = node.execute("SELECT length(m) FROM o_test_main_update WHERE id = 1;")
        self.assertEqual(res[0][0], len(large))

        # Verify updated data content
        res = node.execute("SELECT m FROM o_test_main_update WHERE id = 1;")
        self.assertEqual(res[0][0], large)

        # Verify that large value is toasted out-of-line
        res = node.execute("SELECT pg_column_toast_chunk_id(m) FROM o_test_main_update WHERE id = 1;")
        large_chunk_id = res[0][0]

        # Large incompressible value should be toasted out-of-line
        self.assertIsNotNone(
            large_chunk_id,
            "Large incompressible value should be toasted out-of-line, but chunk_id is NULL"
        )

        # Update back to small value
        small2 = gen_str(50, 2)
        node.safe_psql('postgres', "UPDATE o_test_main_update SET m = '%s' WHERE id = 1;" % small2)

        # Verify the small value again
        res = node.execute("SELECT m FROM o_test_main_update WHERE id = 1;")
        self.assertEqual(res[0][0], small2)

        # Verify small value is not toasted out-of-line after update
        res = node.execute("SELECT pg_column_toast_chunk_id(m) FROM o_test_main_update WHERE id = 1;")
        small2_chunk_id = res[0][0]
        self.assertIsNone(
            small2_chunk_id,
            f"Small value should not be toasted out-of-line after update, but chunk_id={small2_chunk_id}"
        )

        node.stop()
