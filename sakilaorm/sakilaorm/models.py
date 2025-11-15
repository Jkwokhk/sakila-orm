from django.db import models


class Country(models.Model):
    country_id = models.AutoField(primary_key=True)
    country = models.CharField(max_length=50)
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'country'


class City(models.Model):
    city_id = models.AutoField(primary_key=True)
    city = models.CharField(max_length=50)
    country = models.ForeignKey(Country, on_delete=models.DO_NOTHING, db_column='country_id')
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'city'


class Address(models.Model):
    address_id = models.AutoField(primary_key=True)
    address = models.CharField(max_length=50)
    address2 = models.CharField(max_length=50, null=True, blank=True)
    district = models.CharField(max_length=20)
    city = models.ForeignKey(City, on_delete=models.DO_NOTHING, db_column='city_id')
    postal_code = models.CharField(max_length=10, null=True, blank=True)
    phone = models.CharField(max_length=20)
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'address'


class Language(models.Model):
    language_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=20)
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'language'


class Film(models.Model):
    film_id = models.AutoField(primary_key=True)
    title = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    release_year = models.IntegerField(null=True, blank=True)
    language = models.ForeignKey(Language, on_delete=models.DO_NOTHING, db_column='language_id', related_name='films')
    original_language = models.ForeignKey(Language, on_delete=models.DO_NOTHING, db_column='original_language_id', null=True, blank=True, related_name='original_films')
    rental_duration = models.IntegerField()
    rental_rate = models.DecimalField(max_digits=4, decimal_places=2)
    length = models.IntegerField(null=True, blank=True)
    replacement_cost = models.DecimalField(max_digits=5, decimal_places=2)
    rating = models.CharField(max_length=10, null=True, blank=True)
    special_features = models.CharField(max_length=255, null=True, blank=True)
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'film'


class Actor(models.Model):
    actor_id = models.AutoField(primary_key=True)
    first_name = models.CharField(max_length=45)
    last_name = models.CharField(max_length=45)
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'actor'


class Category(models.Model):
    category_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=25)
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'category'


class FilmActor(models.Model):
    actor = models.ForeignKey(Actor, on_delete=models.DO_NOTHING, db_column='actor_id', primary_key=True)
    film = models.ForeignKey(Film, on_delete=models.DO_NOTHING, db_column='film_id')
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'film_actor'
        unique_together = (('actor', 'film'),)


class FilmCategory(models.Model):
    film = models.ForeignKey(Film, on_delete=models.DO_NOTHING, db_column='film_id', primary_key=True)
    category = models.ForeignKey(Category, on_delete=models.DO_NOTHING, db_column='category_id')
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'film_category'
        unique_together = (('film', 'category'),)


class Store(models.Model):
    store_id = models.AutoField(primary_key=True)
    manager_staff_id = models.IntegerField()
    address = models.ForeignKey(Address, on_delete=models.DO_NOTHING, db_column='address_id')
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'store'


class Staff(models.Model):
    staff_id = models.AutoField(primary_key=True)
    first_name = models.CharField(max_length=45)
    last_name = models.CharField(max_length=45)
    address = models.ForeignKey(Address, on_delete=models.DO_NOTHING, db_column='address_id')
    picture = models.BinaryField(null=True, blank=True)
    email = models.CharField(max_length=50, null=True, blank=True)
    store = models.ForeignKey(Store, on_delete=models.DO_NOTHING, db_column='store_id')
    active = models.IntegerField()
    username = models.CharField(max_length=16)
    password = models.CharField(max_length=40, null=True, blank=True)
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'staff'


class Customer(models.Model):
    customer_id = models.AutoField(primary_key=True)
    store = models.ForeignKey(Store, on_delete=models.DO_NOTHING, db_column='store_id')
    first_name = models.CharField(max_length=45)
    last_name = models.CharField(max_length=45)
    email = models.CharField(max_length=50, null=True, blank=True)
    address = models.ForeignKey(Address, on_delete=models.DO_NOTHING, db_column='address_id')
    active = models.IntegerField()
    create_date = models.DateTimeField()
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'customer'


class Inventory(models.Model):
    inventory_id = models.AutoField(primary_key=True)
    film = models.ForeignKey(Film, on_delete=models.DO_NOTHING, db_column='film_id')
    store = models.ForeignKey(Store, on_delete=models.DO_NOTHING, db_column='store_id')
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'inventory'


class Rental(models.Model):
    rental_id = models.AutoField(primary_key=True)
    rental_date = models.DateTimeField()
    inventory = models.ForeignKey(Inventory, on_delete=models.DO_NOTHING, db_column='inventory_id')
    customer = models.ForeignKey(Customer, on_delete=models.DO_NOTHING, db_column='customer_id')
    return_date = models.DateTimeField(null=True, blank=True)
    staff = models.ForeignKey(Staff, on_delete=models.DO_NOTHING, db_column='staff_id')
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'rental'


class Payment(models.Model):
    payment_id = models.AutoField(primary_key=True)
    customer = models.ForeignKey(Customer, on_delete=models.DO_NOTHING, db_column='customer_id')
    staff = models.ForeignKey(Staff, on_delete=models.DO_NOTHING, db_column='staff_id')
    rental = models.ForeignKey(Rental, on_delete=models.DO_NOTHING, db_column='rental_id', null=True, blank=True)
    amount = models.DecimalField(max_digits=5, decimal_places=2)
    payment_date = models.DateTimeField()
    last_update = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'payment'


# ===================================
# ANALYTICS MODELS (SQLite - Read/Write)
# ===================================

# Dimensions

class DimDate(models.Model):
    date_key = models.IntegerField(primary_key=True)  # YYYYMMDD format
    date = models.DateField(unique=True)
    year = models.IntegerField()
    quarter = models.IntegerField()
    month = models.IntegerField()
    day_of_month = models.IntegerField()
    day_of_week = models.IntegerField()
    is_weekend = models.IntegerField()

    class Meta:
        managed = True
        db_table = 'dim_date'


class DimFilm(models.Model):
    film_key = models.AutoField(primary_key=True)
    film_id = models.IntegerField(unique=True)
    title = models.CharField(max_length=255)
    rating = models.CharField(max_length=10, null=True, blank=True)
    length = models.IntegerField(null=True, blank=True)
    language = models.CharField(max_length=20)
    release_year = models.IntegerField(null=True, blank=True)
    last_update = models.DateTimeField()

    class Meta:
        managed = True
        db_table = 'dim_film'
        indexes = [
            models.Index(fields=['film_id']),
        ]


class DimActor(models.Model):
    actor_key = models.AutoField(primary_key=True)
    actor_id = models.IntegerField(unique=True)
    first_name = models.CharField(max_length=45)
    last_name = models.CharField(max_length=45)
    last_update = models.DateTimeField()

    class Meta:
        managed = True
        db_table = 'dim_actor'
        indexes = [
            models.Index(fields=['actor_id']),
        ]


class DimCategory(models.Model):
    category_key = models.AutoField(primary_key=True)
    category_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=25)
    last_update = models.DateTimeField()

    class Meta:
        managed = True
        db_table = 'dim_category'
        indexes = [
            models.Index(fields=['category_id']),
        ]


class DimStore(models.Model):
    store_key = models.AutoField(primary_key=True)
    store_id = models.IntegerField(unique=True)
    city = models.CharField(max_length=50)
    country = models.CharField(max_length=50)
    last_update = models.DateTimeField()

    class Meta:
        managed = True
        db_table = 'dim_store'
        indexes = [
            models.Index(fields=['store_id']),
        ]


class DimCustomer(models.Model):
    customer_key = models.AutoField(primary_key=True)
    customer_id = models.IntegerField(unique=True)
    first_name = models.CharField(max_length=45)
    last_name = models.CharField(max_length=45)
    active = models.IntegerField()
    city = models.CharField(max_length=50)
    country = models.CharField(max_length=50)
    last_update = models.DateTimeField()

    class Meta:
        managed = True
        db_table = 'dim_customer'
        indexes = [
            models.Index(fields=['customer_id']),
        ]


# Bridges

class BridgeFilmActor(models.Model):
    film_key = models.IntegerField()
    actor_key = models.IntegerField()

    class Meta:
        managed = True
        db_table = 'bridge_film_actor'
        unique_together = (('film_key', 'actor_key'),)
        indexes = [
            models.Index(fields=['film_key']),
            models.Index(fields=['actor_key']),
        ]


class BridgeFilmCategory(models.Model):
    film_key = models.IntegerField()
    category_key = models.IntegerField()

    class Meta:
        managed = True
        db_table = 'bridge_film_category'
        unique_together = (('film_key', 'category_key'),)
        indexes = [
            models.Index(fields=['film_key']),
            models.Index(fields=['category_key']),
        ]


# Facts

class FactRental(models.Model):
    fact_rental_key = models.AutoField(primary_key=True)
    rental_id = models.IntegerField(unique=True)
    date_key_rented = models.IntegerField()
    date_key_returned = models.IntegerField(null=True, blank=True)
    film_key = models.IntegerField()
    store_key = models.IntegerField()
    customer_key = models.IntegerField()
    staff_id = models.IntegerField()
    rental_duration_days = models.IntegerField(null=True, blank=True)

    class Meta:
        managed = True
        db_table = 'fact_rental'
        indexes = [
            models.Index(fields=['rental_id']),
            models.Index(fields=['date_key_rented']),
            models.Index(fields=['film_key']),
            models.Index(fields=['store_key']),
            models.Index(fields=['customer_key']),
        ]


class FactPayment(models.Model):
    fact_payment_key = models.AutoField(primary_key=True)
    payment_id = models.IntegerField(unique=True)
    date_key_paid = models.IntegerField()
    customer_key = models.IntegerField()
    store_key = models.IntegerField()
    staff_id = models.IntegerField()
    amount = models.DecimalField(max_digits=5, decimal_places=2)

    class Meta:
        managed = True
        db_table = 'fact_payment'
        indexes = [
            models.Index(fields=['payment_id']),
            models.Index(fields=['date_key_paid']),
            models.Index(fields=['customer_key']),
            models.Index(fields=['store_key']),
        ]


# Utility

class SyncState(models.Model):
    table_name = models.CharField(max_length=100, primary_key=True)
    last_sync_timestamp = models.DateTimeField()

    class Meta:
        managed = True
        db_table = 'sync_state'
