"""
Reputation API views — request/response only, бизнес-логика в services.py.
"""
from __future__ import annotations

import logging

from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.tenant.reputation.models import ExternalReview, ReviewSource, ReviewStatus

from . import services
from .serializers import (
    ExternalReviewSerializer,
    ReputationSyncStateSerializer,
    SaveReplySerializer,
    SyncSerializer,
)

logger = logging.getLogger(__name__)


class ReviewListView(APIView):
    """
    GET /api/v1/reputation/reviews/

    Query params:
      branch    — Branch.id (опционально)
      source    — yandex | gis (опционально)
      status    — new | seen | answered | ignored (опционально, можно через запятую)
      limit     — 1..200, default 50
      offset    — default 0
    """
    permission_classes = [IsAdminUser]

    def get(self, request):
        qs = (
            ExternalReview.objects
            .select_related('branch', 'branch__config')
            .order_by('-published_at', '-fetched_at')
        )
        branch = request.query_params.get('branch')
        if branch:
            qs = qs.filter(branch_id=branch)
        source = request.query_params.get('source')
        if source:
            qs = qs.filter(source=source)
        status_param = request.query_params.get('status')
        if status_param:
            statuses = [s.strip() for s in status_param.split(',') if s.strip()]
            qs = qs.filter(status__in=statuses)

        try:
            limit = max(1, min(int(request.query_params.get('limit', 50)), 200))
        except ValueError:
            limit = 50
        try:
            offset = max(0, int(request.query_params.get('offset', 0)))
        except ValueError:
            offset = 0

        total = qs.count()
        data = ExternalReviewSerializer(qs[offset:offset + limit], many=True).data
        return Response({'total': total, 'limit': limit, 'offset': offset, 'items': data})


class ReviewMarkSeenView(APIView):
    permission_classes = [IsAdminUser]

    def post(self, request, pk: int):
        review = get_object_or_404(ExternalReview, pk=pk)
        services.mark_seen(review)
        return Response(ExternalReviewSerializer(review).data)


class ReviewIgnoreView(APIView):
    permission_classes = [IsAdminUser]

    def post(self, request, pk: int):
        review = get_object_or_404(ExternalReview, pk=pk)
        services.ignore(review)
        return Response(ExternalReviewSerializer(review).data)


class ReviewSaveReplyView(APIView):
    permission_classes = [IsAdminUser]

    def post(self, request, pk: int):
        review = get_object_or_404(ExternalReview, pk=pk)
        ser = SaveReplySerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        services.save_reply(review, reply_text=ser.validated_data['reply_text'], user=request.user)
        return Response(ExternalReviewSerializer(review).data)


class ReviewGenerateReplyView(APIView):
    permission_classes = [IsAdminUser]

    def post(self, request, pk: int):
        review = get_object_or_404(ExternalReview, pk=pk)
        try:
            suggestion = services.generate_reply_suggestion(review)
        except RuntimeError as exc:
            return Response({'detail': str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except Exception as exc:
            logger.exception('generate_reply failed review=%s', pk)
            return Response(
                {'detail': f'AI error: {type(exc).__name__}'},
                status=status.HTTP_502_BAD_GATEWAY,
            )
        return Response({'suggestion': suggestion})


class ReputationSyncView(APIView):
    """
    POST /api/v1/reputation/sync/
        { "branch_id": 42, "source": "yandex" }

    Диспатчит Celery-задачу на обновление отзывов для одной пары (branch, source),
    либо для обоих источников, если source не передан. Возвращает 202
    с перечнем поставленных задач.
    """
    permission_classes = [IsAdminUser]

    def post(self, request):
        from django.db import connection
        from apps.tenant.reputation.tasks import fetch_reviews_for_branch_task

        ser = SyncSerializer(data=request.data)
        ser.is_valid(raise_exception=True)

        branch_id = ser.validated_data['branch_id']
        sources = (
            [ser.validated_data['source']]
            if ser.validated_data.get('source') else
            [ReviewSource.YANDEX, ReviewSource.GIS]
        )
        schema_name = connection.schema_name  # django-tenants подставит текущую схему
        dispatched = []
        for src in sources:
            fetch_reviews_for_branch_task.delay(
                branch_id=branch_id, source=src, schema_name=schema_name,
            )
            dispatched.append(src)

        return Response({'dispatched': dispatched, 'schema': schema_name}, status=status.HTTP_202_ACCEPTED)


class ReputationSyncStatesView(APIView):
    """GET /api/v1/reputation/sync-states/ — состояние последней синхронизации по филиалам."""
    permission_classes = [IsAdminUser]

    def get(self, request):
        from apps.tenant.reputation.models import ReputationSyncState

        qs = (
            ReputationSyncState.objects
            .select_related('branch')
            .order_by('branch__name', 'source')
        )
        return Response(ReputationSyncStateSerializer(qs, many=True).data)
