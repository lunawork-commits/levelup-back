from django.contrib.auth.backends import ModelBackend


class RoleBasedBackend(ModelBackend):
    """
    Расширяет стандартный бэкенд: NETWORK_ADMIN получает все права
    на модели внутри тенанта (аналог is_superuser, но только в рамках
    своих Company). Это заменяет необходимость вручную назначать
    Django Groups / model permissions для каждого NETWORK_ADMIN.
    """

    def has_perm(self, user_obj, perm, obj=None):
        if not user_obj.is_active:
            return False
        # SUPERADMIN — всё разрешено (стандартное поведение superuser)
        if user_obj.is_superuser:
            return True
        # SUPERADMIN и NETWORK_ADMIN — все права на любые модели
        if getattr(user_obj, 'role', None) in ('superadmin', 'network_admin'):
            return True
        return False

    def has_module_perms(self, user_obj, app_label):
        if not user_obj.is_active:
            return False
        if user_obj.is_superuser:
            return True
        if getattr(user_obj, 'role', None) in ('superadmin', 'network_admin'):
            return True
        return False
