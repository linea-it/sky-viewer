from django.shortcuts import render
import logging


def saml2_template_failure(request, exception=None, status=403, **kwargs):
    """ Renders a simple template with an error message. """
    logger = logging.getLogger("djangosaml2")
    logger.info("------------------------------------------")
    logger.info("saml2_template_failure()")
    logger.info(f"request: {request}")
    logger.info(f"exception: {exception}")
    logger.info(f"status: {status}")
    logger.info(f"kwargs: {kwargs}")

    idp_name = request.session['idp_name']
    needs_registration = request.session.get('needs_registration', False)
    user_status = request.session.get('user_status')

    logger.info(f"idp_name: {idp_name} type: {type(idp_name)}")
    logger.info(f"needs_registration: {needs_registration}")
    logger.info(f"user_status: {user_status}")

    # Se o usuario não possui o uid do linea, redireciona para a pagina de registro
    if needs_registration:

        if idp_name == 'rubin_oidc':
            logger.info("Redirecting to Rubin registration page.")
            return render(request, 'djangosaml2/rubin_need_registration.html', {'exception': exception}, status=status)
        
        logger.info("Redirecting to LIneA registration page.")
        return render(request, 'djangosaml2/linea_need_registration.html', {'exception': exception}, status=status)

    # Se o usuario ainda não tiver com o status de aprovado, redireciona para a pagina de aguardando aprovação
    if user_status in ['PendingApproval', 'Pending']:
        logger.info(f"User status is {user_status}. Redirecting to waiting aproval error page.")
        return render(request, 'djangosaml2/waiting_approval.html', {'exception': exception}, status=status)

    return render(request, 'djangosaml2/login_error.html', {'exception': exception}, status=status)
