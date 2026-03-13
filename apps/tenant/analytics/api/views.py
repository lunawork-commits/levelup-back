"""
Analytics API views — request/response only, no business logic.
"""
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .serializers import StatsQuerySerializer, RFQuerySerializer
from . import services


class GeneralStatsAPIView(APIView):
    """
    GET /api/v1/analytics/stats/

    Query params:
      branch_ids — comma-separated Branch PKs (omit = all branches)
      period     — today | 7d | 30d | 90d | year | all  (default: 30d)
      start      — YYYY-MM-DD  (overrides period)
      end        — YYYY-MM-DD  (overrides period)
    """

    def get(self, request):
        ser = StatsQuerySerializer(data=request.query_params)
        if not ser.is_valid():
            return Response(ser.errors, status=status.HTTP_400_BAD_REQUEST)

        branch_ids = ser.validated_data['branch_ids'] or None
        start_date = ser.validated_data['start']
        end_date   = ser.validated_data['end']

        stats  = services.get_general_stats(branch_ids, start_date, end_date)
        charts = services.get_chart_data(branch_ids, start_date, end_date)

        return Response({
            'stats':  stats,
            'charts': charts,
            'meta': {
                'start':      str(start_date),
                'end':        str(end_date),
                'branch_ids': branch_ids or [],
            },
        })


class RFStatsAPIView(APIView):
    """
    GET /api/v1/analytics/rf/

    Query params:
      branch_ids — comma-separated Branch PKs (omit = all branches)
      mode       — restaurant | delivery (default: restaurant)
      trend_days — number of days for trend chart (7–365, default: 30)
      r_score    — when combined with f_score, returns guest list for that cell
      f_score    — see r_score
    """

    def get(self, request):
        ser = RFQuerySerializer(data=request.query_params)
        if not ser.is_valid():
            return Response(ser.errors, status=status.HTTP_400_BAD_REQUEST)

        branch_ids = ser.validated_data['branch_ids'] or None
        mode       = ser.validated_data['mode']
        trend_days = ser.validated_data['trend_days']
        r_score    = ser.validated_data.get('r_score')
        f_score    = ser.validated_data.get('f_score')

        # Guest list for a specific matrix cell
        if r_score is not None and f_score is not None:
            guests = services.get_rf_segment_guests(branch_ids, r_score, f_score, mode=mode)
            matrix = services.get_rf_matrix(branch_ids, mode=mode)
            cell   = matrix['cells'].get(f'{r_score}_{f_score}', {})
            return Response({
                'guests':       guests,
                'segment_name': cell.get('segment_name', '—'),
                'count':        cell.get('count', 0),
            })

        return Response({
            'matrix':     services.get_rf_matrix(branch_ids, mode=mode),
            'trend':      services.get_rf_snapshot_trend(branch_ids, days=trend_days, mode=mode),
            'migrations': services.get_rf_migration_summary(branch_ids, days=trend_days, mode=mode),
        })


class RecalculateRFView(APIView):
    """
    POST /api/v1/analytics/rf/recalculate/

    Synchronously recalculates RF scores for the given branches and mode.
    Intended for manual runs from the admin dashboard.

    Body (JSON or form):
      mode       — restaurant | delivery  (default: restaurant)
      branch_ids — comma-separated Branch PKs (omit = all active branches)
    """

    def post(self, request):
        ser = RFQuerySerializer(data=request.data)
        if not ser.is_valid():
            return Response(ser.errors, status=status.HTTP_400_BAD_REQUEST)

        branch_ids = ser.validated_data['branch_ids'] or None
        mode       = ser.validated_data['mode']

        result = services.recalculate_rf_scores(branch_ids=branch_ids, mode=mode)
        return Response(result, status=status.HTTP_200_OK)


class BranchListAPIView(APIView):
    """
    GET /api/v1/analytics/branches/

    Returns all active branches for the branch-filter UI.
    """

    def get(self, request):
        return Response(services.get_branches_list())
