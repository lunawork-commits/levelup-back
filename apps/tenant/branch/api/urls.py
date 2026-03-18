from django.urls import path

from .views import (
    BranchInfoView, ClientView, EmployeeView, PromotionView, TransactionsView,
    TestimonialCreateView, VKCallbackView, VKIDProxyView, VKStoryView,
)

urlpatterns = [
    path('branches/<int:branch_id>/', BranchInfoView.as_view(), name='branch-info'),
    path('client/',                   ClientView.as_view(),      name='client'),
    path('employees/',                EmployeeView.as_view(),    name='employees'),
    path('promotions/',               PromotionView.as_view(),   name='promotions'),
    path('transactions/',             TransactionsView.as_view(), name='transactions'),
    path('vk/story/',                 VKStoryView.as_view(),     name='vk-story'),
    path('testimonials/',             TestimonialCreateView.as_view(), name='testimonials-create'),
    path('vk/callback/',              VKCallbackView.as_view(),  name='vk-callback'),
    path('vkid-proxy/<path:vk_path>', VKIDProxyView.as_view(),   name='vkid-proxy'),
]
