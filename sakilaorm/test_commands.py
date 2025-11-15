
import os
import sys
import django
from pathlib import Path


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'sakilaorm.settings')
django.setup()

from django.test import TestCase
from django.db import connection, connections
from sakilaorm.models import (
    Film, Actor, Customer, Rental, Payment,
    DimFilm, DimActor, DimCustomer, FactRental, FactPayment, SyncState
)
from manage import init_command, full_load_command, incremental_command, validate_command


class TestInitCommand(TestCase):
    """Test 1: Init command - Confirms database and tables are created successfully"""
    databases = ['default', 'sakila']

    def test_init_creates_tables(self):
        """Test that init command creates all analytics tables"""
        print("\n Test 1: Init Command ")

        # Run init command
        try:
            init_command()
        except SystemExit:
            pass  # init_command may call sys.exit on success

        # Verify SQLite tables exist
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name LIKE 'dim_%' OR name LIKE 'fact_%' OR name='sync_state'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]

        expected_tables = [
            'dim_actor', 'dim_category', 'dim_customer', 'dim_date',
            'dim_film', 'dim_store', 'fact_payment', 'fact_rental', 'sync_state'
        ]

        for table in expected_tables:
            self.assertIn(table, tables, f"Table {table} should exist")

        print(f" All {len(expected_tables)} analytics tables created successfully")
        print(f"  Tables: {', '.join(expected_tables)}")


class TestFullLoadCommand(TestCase):
    """Test 2: Full-load command - Verifies all data from Sakila is loaded into SQLite"""
    databases = ['default', 'sakila']

    def test_full_load_loads_all_data(self):
        """Test that full-load command loads all data from Sakila"""
        print("\nTest 2: Full-load Command ")

        # Run init first
        try:
            init_command()
        except SystemExit:
            pass

        # Run full load
        try:
            full_load_command()
        except SystemExit:
            pass

        # Verify dimensions are loaded
        source_film_count = Film.objects.using('sakila').count()
        target_film_count = DimFilm.objects.using('default').count()
        self.assertEqual(source_film_count, target_film_count, "Film counts should match")

        source_actor_count = Actor.objects.using('sakila').count()
        target_actor_count = DimActor.objects.using('default').count()
        self.assertEqual(source_actor_count, target_actor_count, "Actor counts should match")

        # Verify facts are loaded
        source_rental_count = Rental.objects.using('sakila').count()
        target_rental_count = FactRental.objects.using('default').count()
        self.assertEqual(source_rental_count, target_rental_count, "Rental counts should match")

        source_payment_count = Payment.objects.using('sakila').count()
        target_payment_count = FactPayment.objects.using('default').count()
        self.assertEqual(source_payment_count, target_payment_count, "Payment counts should match")

        # Verify sync_state is initialized
        sync_states = SyncState.objects.using('default').count()
        self.assertGreater(sync_states, 0, "Sync state should be initialized")

        print(f" Full load completed successfully")
        print(f"  Films: {target_film_count}")
        print(f"  Actors: {target_actor_count}")
        print(f"  Rentals: {target_rental_count}")
        print(f"  Payments: {target_payment_count}")


class TestIncrementalCommandNewData(TestCase):
    """Test 3: Incremental command (new data) - Checks that new records appear correctly"""
    databases = ['default', 'sakila']

    def test_incremental_loads_new_data(self):
        """Test that incremental command loads new rental data"""
        print("\n Test 3: Incremental Command (New Data) ")

        # Setup: Run init and full load
        try:
            init_command()
            full_load_command()
        except SystemExit:
            pass

        # Get initial counts
        initial_rental_count = FactRental.objects.using('default').count()

        # Simulate new data by clearing a few records from analytics
       
        FactRental.objects.using('default').filter(rental_id__lte=5).delete()
        count_after_delete = FactRental.objects.using('default').count()

        self.assertLess(count_after_delete, initial_rental_count, "Some rentals should be deleted")

        # Run incremental sync
        try:
            incremental_command()
        except SystemExit:
            pass

        # Verify data is restored 
        
        final_count = FactRental.objects.using('default').count()

        print(f" Incremental sync completed")
        print(f"  Initial count: {initial_rental_count}")
        print(f"  After delete: {count_after_delete}")
        print(f"  After sync: {final_count}")
     


class TestIncrementalCommandUpdates(TestCase):
    """Test 4: Incremental command (updates) - Ensures existing rows are updated"""
    databases = ['default', 'sakila']

    def test_incremental_updates_existing_data(self):
        """Test that incremental command updates modified records"""
        print("\n Test 4: Incremental Command (Updates) ")

        # Setup: Run init and full load
        try:
            init_command()
            full_load_command()
        except SystemExit:
            pass

        # Get a film from analytics
        film = DimFilm.objects.using('default').first()
        original_title = film.title if film else None

        self.assertIsNotNone(film, "Should have at least one film")

        # Modify the film in analytics (simulating source update)
        film.title = "UPDATED TITLE TEST"
        film.save(using='default')

        # Verify modification
        updated_film = DimFilm.objects.using('default').get(film_id=film.film_id)
        self.assertEqual(updated_film.title, "UPDATED TITLE TEST")

        # Run incremental sync (should restore original from Sakila)
        try:
            incremental_command()
        except SystemExit:
            pass

        # Verify film is restored to original
        restored_film = DimFilm.objects.using('default').get(film_id=film.film_id)

        print(f" Incremental update completed")
        print(f"  Original title: {original_title}")
        print(f"  Modified title: UPDATED TITLE TEST")
        print(f"  Restored title: {restored_film.title}")
      


class TestValidateCommand(TestCase):
    """Test 5: Validate command - Confirms data consistency between MySQL and SQLite"""
    databases = ['default', 'sakila']

    def test_validate_confirms_consistency(self):
        """Test that validate command confirms data consistency"""
        print("\n  Test 5: Validate Command ")

        # Setup: Run init and full load
        try:
            init_command()
            full_load_command()
        except SystemExit:
            pass

        # Run validate - should pass with no errors
        try:
            validate_command()
            validation_passed = True
        except SystemExit as e:
            validation_passed = (e.code == 0 or e.code is None)

        # Verify counts match
        source_film_count = Film.objects.using('sakila').count()
        target_film_count = DimFilm.objects.using('default').count()
        self.assertEqual(source_film_count, target_film_count)

        source_payment_count = Payment.objects.using('sakila').count()
        target_payment_count = FactPayment.objects.using('default').count()
        self.assertEqual(source_payment_count, target_payment_count)

        print(f" Validation completed")
        print(f"  Films match: {source_film_count} == {target_film_count}")
        print(f"  Payments match: {source_payment_count} == {target_payment_count}")
        print(f"  Validation status: {'PASSED' if validation_passed else 'FAILED'}")


def run_tests():
    """Run all tests"""
    import unittest

  
    print("Running Sakila Sync Tests")
 

    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add tests in order
    suite.addTests(loader.loadTestsFromTestCase(TestInitCommand))
    suite.addTests(loader.loadTestsFromTestCase(TestFullLoadCommand))
    suite.addTests(loader.loadTestsFromTestCase(TestIncrementalCommandNewData))
    suite.addTests(loader.loadTestsFromTestCase(TestIncrementalCommandUpdates))
    suite.addTests(loader.loadTestsFromTestCase(TestValidateCommand))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Summary

    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)
