class DatabaseRouter:

    def db_for_read(self, model, **hints):
        """
        read from sakila 
        """
        if model._meta.app_label == 'sakilaorm':
            # If model is unmanaged use sakila
            if not model._meta.managed:
                return 'sakila'
        return 'default'

    def db_for_write(self, model, **hints):
        """
        write to sakila 
        """
        if model._meta.app_label == 'sakilaorm':
            # If model is unmanaged use sakila
            if not model._meta.managed:
                return 'sakila'
        return 'default'

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if both models are in the sakilaorm app.
        """
        if obj1._meta.app_label == 'sakilaorm' and obj2._meta.app_label == 'sakilaorm':
            return True
        return None
