
import os
import sys
import django


def init_command():
    """Initialize the analytics db"""
    print("Initializing analytics db")

    try:
        from django.core.management import call_command
        from django.db import connections

        # Verify MySQL connection
        print("Connecting to Sakila")
        sakila_conn = connections['sakila']
        with sakila_conn.cursor() as cursor:
            cursor.execute("SELECT DATABASE()")
            db_name = cursor.fetchone()[0]
            print(f"Connected to MySQL database: {db_name}")

        # Create SQLite tables for analytics models
        print("Creating analytics tables")
        call_command('migrate', '--database=default', '--run-syncdb', verbosity=1)

        print("Analytics db initialized")

    except Exception as e:
        print(f"Error initializing db: {e}")
        sys.exit(1)


def full_load_command():
    """Load all source data from Sakila to SQLite analytics"""
    print("Starting full load from Sakila to analytics db")

    try:
        from django.db import transaction, connections
        from sakilaorm.models import (
            # Source models
            Film, Actor, Category, FilmActor, FilmCategory,
            Store, Customer, Rental, Payment, Language,
            # Analytics models
            DimDate, DimFilm, DimActor, DimCategory, DimStore, DimCustomer,
            BridgeFilmActor, BridgeFilmCategory,
            FactRental, FactPayment, SyncState
        )
        from datetime import datetime, date
        from django.utils import timezone

        # Helper function to generate date_key from date
        def get_date_key(dt):
            if dt is None:
                return None
            if isinstance(dt, datetime):
                dt = dt.date()
            return int(dt.strftime('%Y%m%d'))

        # Helper function to calculate rental duration
        def calculate_rental_duration(rental_date, return_date):
            if rental_date and return_date:
                return (return_date - rental_date).days
            return None

        with transaction.atomic(using='default'):
            print("Loading dimensions")

            # Load dim_date
            print("  Loading dim_date")
            dates_to_create = set()

            # Collect all unique dates from rentals and payments
            for rental in Rental.objects.using('sakila').all():
                if rental.rental_date:
                    dates_to_create.add(rental.rental_date.date())
                if rental.return_date:
                    dates_to_create.add(rental.return_date.date())

            for payment in Payment.objects.using('sakila').all():
                if payment.payment_date:
                    dates_to_create.add(payment.payment_date.date())

            # Create dim_date records
            for dt in dates_to_create:
                date_key = get_date_key(dt)
                DimDate.objects.using('default').update_or_create(
                    date_key=date_key,
                    defaults={
                        'date': dt,
                        'year': dt.year,
                        'quarter': (dt.month - 1) // 3 + 1,
                        'month': dt.month,
                        'day_of_month': dt.day,
                        'day_of_week': dt.weekday(),
                        'is_weekend': 1 if dt.weekday() >= 5 else 0,
                    }
                )
            print(f"    Loaded {len(dates_to_create)} dates")

            # Load dim_film
            print("  Loading dim_film")
            film_count = 0
            film_key_mapping = {}  # film_id -> film_key

            for film in Film.objects.using('sakila').select_related('language').all():
                dim_film, created = DimFilm.objects.using('default').update_or_create(
                    film_id=film.film_id,
                    defaults={
                        'title': film.title,
                        'rating': film.rating,
                        'length': film.length,
                        'language': film.language.name,
                        'release_year': film.release_year,
                        'last_update': film.last_update,
                    }
                )
                film_key_mapping[film.film_id] = dim_film.film_key
                film_count += 1
            print(f"    Loaded {film_count} films")

            # Load dim_actor
            print("  Loading dim_actor")
            actor_count = 0
            actor_key_mapping = {}  # actor_id -> actor_key

            for actor in Actor.objects.using('sakila').all():
                dim_actor, created = DimActor.objects.using('default').update_or_create(
                    actor_id=actor.actor_id,
                    defaults={
                        'first_name': actor.first_name,
                        'last_name': actor.last_name,
                        'last_update': actor.last_update,
                    }
                )
                actor_key_mapping[actor.actor_id] = dim_actor.actor_key
                actor_count += 1
            print(f"    Loaded {actor_count} actors")

            # Load dim_category
            print("  Loading dim_category")
            category_count = 0
            category_key_mapping = {}  # category_id -> category_key

            for category in Category.objects.using('sakila').all():
                dim_category, created = DimCategory.objects.using('default').update_or_create(
                    category_id=category.category_id,
                    defaults={
                        'name': category.name,
                        'last_update': category.last_update,
                    }
                )
                category_key_mapping[category.category_id] = dim_category.category_key
                category_count += 1
            print(f"    Loaded {category_count} categories")

            # Load dim_store
            print("  Loading dim_store")
            store_count = 0
            store_key_mapping = {}  # store_id -> store_key

            for store in Store.objects.using('sakila').select_related('address__city__country').all():
                dim_store, created = DimStore.objects.using('default').update_or_create(
                    store_id=store.store_id,
                    defaults={
                        'city': store.address.city.city,
                        'country': store.address.city.country.country,
                        'last_update': store.last_update,
                    }
                )
                store_key_mapping[store.store_id] = dim_store.store_key
                store_count += 1
            print(f"    Loaded {store_count} stores")

            # Load dim_customer
            print("  Loading dim_customer")
            customer_count = 0
            customer_key_mapping = {}  # customer_id -> customer_key

            for customer in Customer.objects.using('sakila').select_related('address__city__country').all():
                dim_customer, created = DimCustomer.objects.using('default').update_or_create(
                    customer_id=customer.customer_id,
                    defaults={
                        'first_name': customer.first_name,
                        'last_name': customer.last_name,
                        'active': customer.active,
                        'city': customer.address.city.city,
                        'country': customer.address.city.country.country,
                        'last_update': customer.last_update,
                    }
                )
                customer_key_mapping[customer.customer_id] = dim_customer.customer_key
                customer_count += 1
            print(f"    Loaded {customer_count} customers")

            # Load bridges
            print("Loading bridges")

            # Load bridge_film_actor
            print("  Loading bridge_film_actor")
            bridge_fa_count = 0
            for film_actor in FilmActor.objects.using('sakila').values('actor_id', 'film_id'):
                film_key = film_key_mapping.get(film_actor['film_id'])
                actor_key = actor_key_mapping.get(film_actor['actor_id'])
                if film_key and actor_key:
                    BridgeFilmActor.objects.using('default').update_or_create(
                        film_key=film_key,
                        actor_key=actor_key,
                    )
                    bridge_fa_count += 1
            print(f"    Loaded {bridge_fa_count} film-actor relationships")

            # Load bridge_film_category
            print("  Loading bridge_film_category")
            bridge_fc_count = 0
            for film_category in FilmCategory.objects.using('sakila').values('film_id', 'category_id'):
                film_key = film_key_mapping.get(film_category['film_id'])
                category_key = category_key_mapping.get(film_category['category_id'])
                if film_key and category_key:
                    BridgeFilmCategory.objects.using('default').update_or_create(
                        film_key=film_key,
                        category_key=category_key,
                    )
                    bridge_fc_count += 1
            print(f"    Loaded {bridge_fc_count} film-category relationships")

            # Load facts
            print("Loading facts")

            # Load fact_rental
            print("  Loading fact_rental")
            rental_count = 0
            for rental in Rental.objects.using('sakila').select_related('inventory__film', 'inventory__store', 'customer').all():
                film_key = film_key_mapping.get(rental.inventory.film_id)
                store_key = store_key_mapping.get(rental.inventory.store_id)
                customer_key = customer_key_mapping.get(rental.customer_id)

                if film_key and store_key and customer_key:
                    FactRental.objects.using('default').update_or_create(
                        rental_id=rental.rental_id,
                        defaults={
                            'date_key_rented': get_date_key(rental.rental_date),
                            'date_key_returned': get_date_key(rental.return_date),
                            'film_key': film_key,
                            'store_key': store_key,
                            'customer_key': customer_key,
                            'staff_id': rental.staff_id,
                            'rental_duration_days': calculate_rental_duration(rental.rental_date, rental.return_date),
                        }
                    )
                    rental_count += 1
            print(f"    Loaded {rental_count} rentals")

            # Load fact_payment
            print("  Loading fact_payment")
            payment_count = 0
            for payment in Payment.objects.using('sakila').select_related('customer', 'rental__inventory__store').all():
                customer_key = customer_key_mapping.get(payment.customer_id)
                store_key = None
                if payment.rental and payment.rental.inventory:
                    store_key = store_key_mapping.get(payment.rental.inventory.store_id)

                if customer_key and store_key:
                    FactPayment.objects.using('default').update_or_create(
                        payment_id=payment.payment_id,
                        defaults={
                            'date_key_paid': get_date_key(payment.payment_date),
                            'customer_key': customer_key,
                            'store_key': store_key,
                            'staff_id': payment.staff_id,
                            'amount': payment.amount,
                        }
                    )
                    payment_count += 1
            print(f"    Loaded {payment_count} payments")

            # Initialize sync_state
            print("Initializing sync state")
            current_time = timezone.now()
            for table_name in ['film', 'actor', 'category', 'store', 'customer', 'rental', 'payment']:
                SyncState.objects.using('default').update_or_create(
                    table_name=table_name,
                    defaults={'last_sync_timestamp': current_time}
                )

        print("Full load completed successfully!")

    except Exception as e:
        print(f"Error during full load: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def incremental_command():
    """Load only new or changed data from Sakila"""
    print("Starting incremental sync from Sakila to analytics db")

    try:
        from django.db import transaction
        from sakilaorm.models import (
            # Source models
            Film, Actor, Category, FilmActor, FilmCategory,
            Store, Customer, Rental, Payment,
            # Analytics models
            DimDate, DimFilm, DimActor, DimCategory, DimStore, DimCustomer,
            BridgeFilmActor, BridgeFilmCategory,
            FactRental, FactPayment, SyncState
        )
        from datetime import datetime
        from django.utils import timezone

        # Helper functions
        def get_date_key(dt):
            if dt is None:
                return None
            if isinstance(dt, datetime):
                dt = dt.date()
            return int(dt.strftime('%Y%m%d'))

        def calculate_rental_duration(rental_date, return_date):
            if rental_date and return_date:
                return (return_date - rental_date).days
            return None

        with transaction.atomic(using='default'):
            current_time = timezone.now()

            # Sync dim_film
            print("Syncing dim_film")
            last_sync = SyncState.objects.using('default').filter(table_name='film').first()
            last_sync_time = last_sync.last_sync_timestamp if last_sync else datetime.min

            updated_films = Film.objects.using('sakila').filter(last_update__gt=last_sync_time).select_related('language')
            film_count = 0
            for film in updated_films:
                DimFilm.objects.using('default').update_or_create(
                    film_id=film.film_id,
                    defaults={
                        'title': film.title,
                        'rating': film.rating,
                        'length': film.length,
                        'language': film.language.name,
                        'release_year': film.release_year,
                        'last_update': film.last_update,
                    }
                )
                film_count += 1
            print(f"  Updated {film_count} films")

            # Sync dim_actor
            print("Syncing dim_actor")
            last_sync = SyncState.objects.using('default').filter(table_name='actor').first()
            last_sync_time = last_sync.last_sync_timestamp if last_sync else datetime.min

            updated_actors = Actor.objects.using('sakila').filter(last_update__gt=last_sync_time)
            actor_count = 0
            for actor in updated_actors:
                DimActor.objects.using('default').update_or_create(
                    actor_id=actor.actor_id,
                    defaults={
                        'first_name': actor.first_name,
                        'last_name': actor.last_name,
                        'last_update': actor.last_update,
                    }
                )
                actor_count += 1
            print(f"  Updated {actor_count} actors")

            # Sync dim_category
            print("Syncing dim_category")
            last_sync = SyncState.objects.using('default').filter(table_name='category').first()
            last_sync_time = last_sync.last_sync_timestamp if last_sync else datetime.min

            updated_categories = Category.objects.using('sakila').filter(last_update__gt=last_sync_time)
            category_count = 0
            for category in updated_categories:
                DimCategory.objects.using('default').update_or_create(
                    category_id=category.category_id,
                    defaults={
                        'name': category.name,
                        'last_update': category.last_update,
                    }
                )
                category_count += 1
            print(f"  Updated {category_count} categories")

            # Sync dim_store
            print("Syncing dim_store")
            last_sync = SyncState.objects.using('default').filter(table_name='store').first()
            last_sync_time = last_sync.last_sync_timestamp if last_sync else datetime.min

            updated_stores = Store.objects.using('sakila').filter(last_update__gt=last_sync_time).select_related('address__city__country')
            store_count = 0
            for store in updated_stores:
                DimStore.objects.using('default').update_or_create(
                    store_id=store.store_id,
                    defaults={
                        'city': store.address.city.city,
                        'country': store.address.city.country.country,
                        'last_update': store.last_update,
                    }
                )
                store_count += 1
            print(f"  Updated {store_count} stores")

            # Sync dim_customer
            print("Syncing dim_customer")
            last_sync = SyncState.objects.using('default').filter(table_name='customer').first()
            last_sync_time = last_sync.last_sync_timestamp if last_sync else datetime.min

            updated_customers = Customer.objects.using('sakila').filter(last_update__gt=last_sync_time).select_related('address__city__country')
            customer_count = 0
            for customer in updated_customers:
                DimCustomer.objects.using('default').update_or_create(
                    customer_id=customer.customer_id,
                    defaults={
                        'first_name': customer.first_name,
                        'last_name': customer.last_name,
                        'active': customer.active,
                        'city': customer.address.city.city,
                        'country': customer.address.city.country.country,
                        'last_update': customer.last_update,
                    }
                )
                customer_count += 1
            print(f"  Updated {customer_count} customers")

            # Sync fact_rental (using rental_date as timestamp)
            print("Syncing fact_rental")
            last_sync = SyncState.objects.using('default').filter(table_name='rental').first()
            last_sync_time = last_sync.last_sync_timestamp if last_sync else datetime.min

            # Get new/updated rentals
            updated_rentals = Rental.objects.using('sakila').filter(
                rental_date__gt=last_sync_time
            ).select_related('inventory__film', 'inventory__store', 'customer')

            rental_count = 0
            new_dates = set()

            for rental in updated_rentals:
                # Collect dates for dim_date
                if rental.rental_date:
                    new_dates.add(rental.rental_date.date())
                if rental.return_date:
                    new_dates.add(rental.return_date.date())

                # Get dimension keys
                film_dim = DimFilm.objects.using('default').filter(film_id=rental.inventory.film_id).first()
                store_dim = DimStore.objects.using('default').filter(store_id=rental.inventory.store_id).first()
                customer_dim = DimCustomer.objects.using('default').filter(customer_id=rental.customer_id).first()

                if film_dim and store_dim and customer_dim:
                    FactRental.objects.using('default').update_or_create(
                        rental_id=rental.rental_id,
                        defaults={
                            'date_key_rented': get_date_key(rental.rental_date),
                            'date_key_returned': get_date_key(rental.return_date),
                            'film_key': film_dim.film_key,
                            'store_key': store_dim.store_key,
                            'customer_key': customer_dim.customer_key,
                            'staff_id': rental.staff_id,
                            'rental_duration_days': calculate_rental_duration(rental.rental_date, rental.return_date),
                        }
                    )
                    rental_count += 1

            # Add new dates to dim_date
            for dt in new_dates:
                date_key = get_date_key(dt)
                DimDate.objects.using('default').update_or_create(
                    date_key=date_key,
                    defaults={
                        'date': dt,
                        'year': dt.year,
                        'quarter': (dt.month - 1) // 3 + 1,
                        'month': dt.month,
                        'day_of_month': dt.day,
                        'day_of_week': dt.weekday(),
                        'is_weekend': 1 if dt.weekday() >= 5 else 0,
                    }
                )

            print(f"  Updated {rental_count} rentals, added {len(new_dates)} new dates")

            # Sync fact_payment (using payment_date as timestamp)
            print("Syncing fact_payment")
            last_sync = SyncState.objects.using('default').filter(table_name='payment').first()
            last_sync_time = last_sync.last_sync_timestamp if last_sync else datetime.min

            updated_payments = Payment.objects.using('sakila').filter(
                payment_date__gt=last_sync_time
            ).select_related('customer', 'rental__inventory__store')

            payment_count = 0
            new_dates_payment = set()

            for payment in updated_payments:
                # Collect dates for dim_date
                if payment.payment_date:
                    new_dates_payment.add(payment.payment_date.date())

                # Get dimension keys
                customer_dim = DimCustomer.objects.using('default').filter(customer_id=payment.customer_id).first()
                store_dim = None
                if payment.rental and payment.rental.inventory:
                    store_dim = DimStore.objects.using('default').filter(store_id=payment.rental.inventory.store_id).first()

                if customer_dim and store_dim:
                    FactPayment.objects.using('default').update_or_create(
                        payment_id=payment.payment_id,
                        defaults={
                            'date_key_paid': get_date_key(payment.payment_date),
                            'customer_key': customer_dim.customer_key,
                            'store_key': store_dim.store_key,
                            'staff_id': payment.staff_id,
                            'amount': payment.amount,
                        }
                    )
                    payment_count += 1

            # Add new dates to dim_date
            for dt in new_dates_payment:
                date_key = get_date_key(dt)
                DimDate.objects.using('default').update_or_create(
                    date_key=date_key,
                    defaults={
                        'date': dt,
                        'year': dt.year,
                        'quarter': (dt.month - 1) // 3 + 1,
                        'month': dt.month,
                        'day_of_month': dt.day,
                        'day_of_week': dt.weekday(),
                        'is_weekend': 1 if dt.weekday() >= 5 else 0,
                    }
                )

            print(f"  Updated {payment_count} payments, added {len(new_dates_payment)} new dates")

            # Update sync_state for all tables
            print("Updating sync state")
            for table_name in ['film', 'actor', 'category', 'store', 'customer', 'rental', 'payment']:
                SyncState.objects.using('default').update_or_create(
                    table_name=table_name,
                    defaults={'last_sync_timestamp': current_time}
                )

        print("Incremental sync completed successfully!")

    except Exception as e:
        print(f"Error during incremental sync: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def validate_command():
    """Verify data consistency between MySQL and SQLite"""
    print("Validating data consistency between Sakila and analytics db")

    try:
        from django.db.models import Sum, Count
        from sakilaorm.models import (
            # Source models
            Film, Actor, Category, Store, Customer, Rental, Payment,
            # Analytics models
            DimFilm, DimActor, DimCategory, DimStore, DimCustomer,
            FactRental, FactPayment
        )
        from datetime import datetime, timedelta
        from django.utils import timezone

        validation_errors = []
        validation_warnings = []

        print("Validating all data")
        print()

        # Validate dimensions
        print("Validating dimensions")

        # Validate films
        source_film_count = Film.objects.using('sakila').count()
        target_film_count = DimFilm.objects.using('default').count()
        print(f"  Films: Source={source_film_count}, Target={target_film_count}")
        if source_film_count != target_film_count:
            validation_warnings.append(f"Film count mismatch: {source_film_count} vs {target_film_count}")

        # Validate actors
        source_actor_count = Actor.objects.using('sakila').count()
        target_actor_count = DimActor.objects.using('default').count()
        print(f"  Actors: Source={source_actor_count}, Target={target_actor_count}")
        if source_actor_count != target_actor_count:
            validation_warnings.append(f"Actor count mismatch: {source_actor_count} vs {target_actor_count}")

        # Validate categories
        source_category_count = Category.objects.using('sakila').count()
        target_category_count = DimCategory.objects.using('default').count()
        print(f"  Categories: Source={source_category_count}, Target={target_category_count}")
        if source_category_count != target_category_count:
            validation_warnings.append(f"Category count mismatch: {source_category_count} vs {target_category_count}")

        # Validate stores
        source_store_count = Store.objects.using('sakila').count()
        target_store_count = DimStore.objects.using('default').count()
        print(f"  Stores: Source={source_store_count}, Target={target_store_count}")
        if source_store_count != target_store_count:
            validation_warnings.append(f"Store count mismatch: {source_store_count} vs {target_store_count}")

        # Validate customers
        source_customer_count = Customer.objects.using('sakila').count()
        target_customer_count = DimCustomer.objects.using('default').count()
        print(f"  Customers: Source={source_customer_count}, Target={target_customer_count}")
        if source_customer_count != target_customer_count:
            validation_warnings.append(f"Customer count mismatch: {source_customer_count} vs {target_customer_count}")

        print()
        print("Validating facts")

        # Validate rentals
        source_rental_count = Rental.objects.using('sakila').count()
        target_rental_count = FactRental.objects.using('default').count()
        print(f"  Rentals: Source={source_rental_count}, Target={target_rental_count}")
        if abs(source_rental_count - target_rental_count) > 0:
            validation_warnings.append(f"Rental count difference: {source_rental_count} vs {target_rental_count}")

        # Validate payments
        source_payment_count = Payment.objects.using('sakila').count()
        target_payment_count = FactPayment.objects.using('default').count()
        print(f"  Payments: Source={source_payment_count}, Target={target_payment_count}")
        if abs(source_payment_count - target_payment_count) > 0:
            validation_warnings.append(f"Payment count difference: {source_payment_count} vs {target_payment_count}")

        # Validate payment totals
        source_payment_total = Payment.objects.using('sakila').aggregate(total=Sum('amount'))['total'] or 0
        target_payment_total = FactPayment.objects.using('default').aggregate(total=Sum('amount'))['total'] or 0

        print(f"  Payment totals: Source=${source_payment_total:.2f}, Target=${target_payment_total:.2f}")

        # Allow small floating point differences
        if abs(float(source_payment_total) - float(target_payment_total)) > 0.01:
            validation_errors.append(
                f"Payment total mismatch: ${source_payment_total:.2f} vs ${target_payment_total:.2f}"
            )

        # Check for duplicates in analytics
        print()
        print("Checking for duplicates")

        duplicate_films = DimFilm.objects.using('default').values('film_id').annotate(
            count=Count('film_id')
        ).filter(count__gt=1)
        if duplicate_films.exists():
            validation_errors.append(f"Found {duplicate_films.count()} duplicate film_ids in dim_film")
        else:
            print("  No duplicate films found")

        duplicate_rentals = FactRental.objects.using('default').values('rental_id').annotate(
            count=Count('rental_id')
        ).filter(count__gt=1)
        if duplicate_rentals.exists():
            validation_errors.append(f"Found {duplicate_rentals.count()} duplicate rental_ids in fact_rental")
        else:
            print("  No duplicate rentals found")

        duplicate_payments = FactPayment.objects.using('default').values('payment_id').annotate(
            count=Count('payment_id')
        ).filter(count__gt=1)
        if duplicate_payments.exists():
            validation_errors.append(f"Found {duplicate_payments.count()} duplicate payment_ids in fact_payment")
        else:
            print("  No duplicate payments found")

        # Summary
        print()
      
        if validation_errors:
            print("VALIDATION FAILED")
            print()
            print("Errors:")
            for error in validation_errors:
                print(f"  {error}")
            if validation_warnings:
                print()
                print("Warnings:")
                for warning in validation_warnings:
                    print(f"   {warning}")
            sys.exit(1)
        elif validation_warnings:
            print("VALIDATION PASSED WITH WARNINGS")
            print()
            print("Warnings:")
            for warning in validation_warnings:
                print(f"   {warning}")
        else:
            print("VALIDATION PASSED")
            print("Data is consistent between source and target databases")

    except Exception as e:
        print(f"Error during validation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'sakilaorm.settings')

    # Handle custom commands
    if len(sys.argv) > 1:
        if sys.argv[1] == 'init':
            django.setup()
            init_command()
            return
        elif sys.argv[1] == 'full-load':
            django.setup()
            full_load_command()
            return
        elif sys.argv[1] == 'incremental':
            django.setup()
            incremental_command()
            return
        elif sys.argv[1] == 'validate':
            django.setup()
            validate_command()
            return

    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django."
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
