from django.conf import settings
from django.contrib.auth import logout
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.decorators import permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response


@api_view(["GET"])
def teste(request):
    if request.method == "GET":
        return Response({"status": "success"})
    return None


@api_view(["GET"])
def logout_user(request):
    logout(request)
    return Response({"message": "Logged out successfully."}, status=status.HTTP_200_OK)


@api_view(["GET"])
@permission_classes([AllowAny])
def environment_settings(request):
    enviroment = settings.ENVIRONMENT_NAME

    is_dev = False
    dev_environments = ["local", "development", "staging"]
    if enviroment in dev_environments:
        is_dev = True

    login_url = settings.LOGIN_URL

    env_settings = {
        "environment": enviroment,
        "base_host": settings.BASE_HOST,
        "login_url": login_url,
        "is_dev": is_dev,
        "version": "1.0.0",
        "build": "12345",
    }
    return Response(env_settings, status=status.HTTP_200_OK)
