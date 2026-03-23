from django.urls import path

from .views import DeliveryCodeView

urlpatterns = [
    path('code/', DeliveryCodeView.as_view(), name='delivery-code'),
]
