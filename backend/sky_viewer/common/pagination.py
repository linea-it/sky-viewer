from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response


class CustomPageNumberPagination(PageNumberPagination):
    # https://www.django-rest-framework.org/api-guide/pagination/#pagenumberpagination
    # https://www.cdrf.co/3.14/rest_framework.pagination/PageNumberPagination.html
    page_size = 100
    page_query_param = "page"
    page_size_query_param = "pageSize"
    max_page_size = 1000

    def get_paginated_response(self, data):
        return Response(
            {
                "count": self.page.paginator.count,
                "results": data,
            },
        )
