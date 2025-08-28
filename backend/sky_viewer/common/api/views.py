from django.conf import settings
from django.contrib.auth import logout
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.decorators import permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt

import logging

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


@csrf_exempt
def nginx_serve_protected_hips(request):
    logger = logging.getLogger("django")
    logger.info("-----------------------------------")
    logger.info("nginx_serve_protected_hips_debug()")

    data = {
        'X-Original-URI': request.META.get('HTTP_X_ORIGINAL_URI'),
        'Cookie': request.META.get('HTTP_COOKIE'),
        'Path': request.path,
        'Method': request.method,
        'META': {k: v for k, v in request.META.items() if k.startswith('HTTP_')},
    }

    logger.debug(data)

    original_uri = request.META.get('HTTP_X_ORIGINAL_URI')
    if original_uri.endswith('/properties') or original_uri.endswith('/Moc.fits'):
        logger.info("Request for properties, temporary always return 200.")
        return HttpResponse({}, content_type="application/json", status=200)
    
    if not request.user.is_authenticated:
        logger.warning("User is not authenticated, returning 403 Forbidden.")
        return HttpResponseForbidden({"message":"User is not authenticated."}, content_type="application/json", status=403)
    
    logger.info(f"User is authenticated: {request.user.username}")

    # Identifie release from the original URI
    if original_uri.find('/lsst/dp02/') > -1:
        # Check if the user has the required group membership for HIPS images
        if not request.user.groups.filter(name='lsst_dp0.2').exists():
            logger.warning(f"User {request.user.username} does not have access to DP02 HIPS images.")
            return HttpResponseForbidden({"message":"User does not have access to DP02 HIPS images."}, content_type="application/json", status=403)
    
    return HttpResponse({}, content_type="application/json", status=200)