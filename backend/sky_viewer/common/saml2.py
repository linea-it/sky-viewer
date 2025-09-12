import logging
from typing import Any
from typing import Optional
from typing import Tuple

from django.contrib.auth.models import Group
from djangosaml2.backends import Saml2Backend
from django.conf import settings

logger = logging.getLogger("djangosaml2")


class LineaSaml2Backend(Saml2Backend):

    def authenticate(self, request, session_info=None, attribute_mapping=None, create_unknown_user=True, assertion_info=None, **kwargs):
        logger.info("=====================================================")
        logger.info("authenticate()")
        logger.info(f"request: {request}")
        logger.info(f"session_info: {session_info}")
        logger.info(f"attribute_mapping: {attribute_mapping}")
        logger.info(f"create_unknown_user: {create_unknown_user}")
        logger.info(f"assertion_info: {assertion_info}")
        # logger.info(f"kwargs: {kwargs}")

        if session_info is None or attribute_mapping is None:
            logger.info("Session info or attribute mapping are None")
            return None

        if "ava" not in session_info:
            logger.error('"ava" key not found in session_info')
            return None

        idp_entityid = session_info["issuer"]
        attributes = self.clean_attributes(session_info["ava"], idp_entityid)

        list_idp_name = attributes.get('schacProjectMembership', None)
        logger.info(f"IDP NAME TYPE: {type(list_idp_name)}")
        idp_name = None
        if list_idp_name:
            idp_name = list_idp_name[0]

        logger.info(f"idp_name: {idp_name}")
        request.session['idp_name'] = idp_name

        if self.get_user_identifier(attributes) is None:
            logger.error("No user identifier found in attributes. Redirect to registration page.")
            # Define flag para redirecionamento para página de registro
            request.session['needs_registration'] = True

            return None


        user = super().authenticate(request, session_info, attribute_mapping, create_unknown_user, assertion_info, **kwargs)

        if user is not None:
            # Tratamento dos grupos que o usuario pertence
            self.setup_groups(user, attributes)

            # Get User Status
            user_status = attributes.get('schacUserStatus', None)
            request.session['user_status'] = user_status
            logger.info(f"User Status: {user_status}")
            if user_status is None:
                logger.error("User status not found in attributes.")
                return None
            
            if user_status in ['PendingApproval', 'Pending']:
                logger.info(f"User status is {user_status}. Redirecting to login error page.")
                return None

            return user
        else:
            logger.error("Authentication failed. User not found or not created.")
            return None

    def is_authorized(
        self,
        attributes: dict,
        attribute_mapping: dict,
        idp_entityid: str,
        assertion_info: dict,
        **kwargs,
    ) -> bool:
        """Hook to allow custom authorization policies based on SAML attributes."""
        logger.info("-----------------------------------------")
        logger.info("Checks if the user is authorized")
        logger.info("is_authorized()")

        logger.info(f"attributes: {attributes}")
        logger.info(f"attribute_mapping: {attribute_mapping}")
        logger.info(f"idp_entityid: {idp_entityid}")
        logger.info(f"assertion_info: {assertion_info}")
        # logger.info(f"kwargs: {kwargs}")
        logger.info("-----------------------------------------")

        # Estamos considerando que todos os usuarios terão cadastro no LIneA mesmo os de outras instituições.
        # O SATOSA do linea sempre vai retornar o uid do usuario um status e o member que são os grupos que o usuario pertence
        # Se o usuario não tiver uid será redirecionado para a tela de cadastro
        # O status será utilizado para dar o feedback correto, cadastro necessário ou em andamento.

        user_lookup_value = self.get_user_identifier(attributes)

        logger.info(
            f"User Lookup Value: {user_lookup_value} Type: {type(user_lookup_value)}"
        )
        if (user_lookup_value in [None, "None", ""]):
            logger.error("No user uid identifier.")

            # Verificar se o usuario é do linea ou do Rubin
            # Verificar o status do cadastro do usuario

            # Redirecionar para a tela de cadastro
            return False

        # Usuario com uid pode prosseguir com a autenticação
        return True


    def _extract_user_identifier_params(
        self, session_info: dict, attributes: dict, attribute_mapping: dict
    ) -> Tuple[str, Optional[Any]]:
        """Returns the attribute to perform a user lookup on, and the value to use for it.
        The value could be the name_id, or any other saml attribute from the request.
        """
        logger.info("-----------------------------------------")
        logger.info("Extract user identifier.")
        logger.info("_extract_user_identifier_params()")
        logger.info(f"session_info: {session_info}")
        logger.info(f"attributes: {attributes}")
        logger.info(f"attribute_mapping: {attribute_mapping}")
        logger.info("-----------------------------------------")
        # Lookup key
        user_lookup_key = 'username'
        logger.info(f"User Lookup Key: {user_lookup_key}")

        # Lookup value
        try:
            user_lookup_value = self.get_user_identifier(attributes)

            logger.info(
                f"User Lookup Value: {user_lookup_value} Type: {type(user_lookup_value)}"
            )
            if (user_lookup_value in [None, "None", ""]):
                logger.error("No identifier to search in COmanager.")
                return user_lookup_key, None

            return user_lookup_key, user_lookup_value

        except Exception as e:
            logger.error("Failed to extract user identifier.")
            logger.error(e)
            return user_lookup_key, None


    def get_user_identifier(self, attributes: dict) -> Any:
        """Returns the user identifier to use for authentication."""
        logger.info("-----------------------------------------")
        logger.info("Get user identifier.")
        logger.info("get_user_identifier()")

        # Lookup key
        user_lookup_key = "uid"
        logger.info(f"User Lookup Key: {user_lookup_key}")

        try:
            # Lookup value
            user_lookup_value = attributes.get(user_lookup_key, None)

            if user_lookup_value and isinstance(user_lookup_value, list):
                user_lookup_value = user_lookup_value[0]

                logger.info(f"User Lookup Value: {user_lookup_value} Type: {type(user_lookup_value)}")
            
            else:
                user_lookup_value = None

        except Exception as e:
            logger.error("Failed to extract user identifier uid.")
            logger.error(e)
            return None

        if (user_lookup_value in [None, "None", ""]):
            logger.error("No identifier uid to search user.")
            return None

        logger.info(f"LDAP UID: {user_lookup_value}")
        return self.clean_user_main_attribute(user_lookup_value)

    def clean_user_main_attribute(self, main_attribute: Any) -> Any:
        """Hook to clean the extracted user-identifying value. No-op by default."""
        main_attribute = main_attribute.replace(".", "_")
        return main_attribute


    def user_can_authenticate(self, user) -> bool:
        """
        Reject users with is_active=False. Custom user models that don't have
        that attribute are allowed.
        """
        is_active = getattr(user, "is_active", None)
        return is_active or is_active is None

    def save_user(self, user, *args, **kwargs):
        logger.info("-----------------------------------------")
        logger.info("save_user()")
        logger.info(f"user: {user}")
        logger.info(f"args: {args}")
        # logger.info(f"kwargs: {kwargs}")

        user = super().save_user(user, *args, **kwargs)
        # # Tratamento dos grupos que o usuario pertence
        # self.setup_groups(user)
        return user

    def setup_groups(self, user, attributes: dict):
        logger.info("Setup User Groups")

        # Add a custom group saml for mark this user make login using djangosaml2.
        groups = ["saml2"]

        # Grupos internos do sistema que não serão removidos do usuario
        internal_groups = getattr(settings, "INTERNAL_GROUPS", [])

        # Recupera os grupos do usuario
        try:
            logger.info("Retriving User Groups.")
            for group in attributes.get("member", []):
                groups.append(group)

        except Exception as e:
            msg = f"Failed on retrive groups. Error: {e}"
            logger.error(msg)

        # Remove the user from all groups that are not specified
        for group in user.groups.all():
            if group.name not in groups and group.name not in internal_groups:
                user.groups.remove(group)
                logger.info(f"User has been removed from the group {group.name}")

        # Add the user to all groups in the shibboleth metadata
        for g in groups:
            group, created = Group.objects.get_or_create(name=g)
            user.groups.add(group)

        logger.info("User has been added to the following groups")
        logger.info(f"Groups: {groups}")

        return user

