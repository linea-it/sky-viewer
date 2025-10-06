import os
import django
from pathlib import Path
import sys
from unittest.mock import MagicMock

# Configure o Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.production')  # ajuste aqui
django.setup()

# This allows easy placement of apps within the interior
# sky_viewer directory.
current_path = Path(__file__).parent.resolve()
sys.path.append(str(current_path / "sky_viewer"))


from sky_viewer.common.saml2 import LineaSaml2Backend  # ajuste aqui para o caminho real da sua classe



def main():
    print("### Testando LineaSaml2Backend ###")
    backend = LineaSaml2Backend()
    
    # Definindo o atributo que será usado como identificador do usuário
    # backend._user_lookup_attribute = 'username'

    # Dados simulados do SAML
    attributes = {
        'displayName': ['Glauber Vila Verde'], 
        'eduPersonTargetedID': ['https://idp01.linea.org.br/idp/shibboleth!https://cilogon.org/shibboleth!SMDvJSJuWXx8RbJ+ttYZ7WTdCo8='], 
        'eduPersonUniqueId': ['http://cilogon.org/serverE/users/196661'], 
        'eduPersonPrincipalName': ['3c23a1f2c7544f5cd31f59e7d7b0a35b@linea.org.br'], 
        'givenName': ['Glauber'], 
        'email': ['glauber.costa@linea.org.br'], 
        'cn': ['Glauber Vila Verde'], 
        'eduPersonEntitlement': ['urn:mace:rediris.es:entitlement:wiki:tfemc2'], 
        'sn': ['Vila Verde'], 
        'o': ['LINEA - Laboratorio Interinstitucional de e-Astronomia'], 
        'schacHomeOrganization': ['https://idp01.linea.org.br/idp/shibboleth'], 
        'uid': ['glauber.costa'], 
        'member': ['public', 'lsst', 'des', 'sdss', 'ton', 'itteam', 'loginapl', 'srvlogin', 'developers', 'hpeapolo', 'srvnodesprod']
    }

    attribute_mapping = {
        'eduPersonUniqueId': ('username',), 
        'givenName': ('first_name',), 
        'sn': ('last_name',), 
        'email': ('email',), 
        'isMemberOf': ('name',)
    }

    idp_entityid = 'https://idp.example.org/idp/shibboleth'
    assertion_info = {}

    session_info = {
        'ava': {
            'displayName': ['Glauber Vila Verde'], 
            'eduPersonTargetedID': ['https://idp01.linea.org.br/idp/shibboleth!https://cilogon.org/shibboleth!SMDvJSJuWXx8RbJ+ttYZ7WTdCo8='], 
            'eduPersonUniqueId': ['http://cilogon.org/serverE/users/196661'], 
            'eduPersonPrincipalName': ['3c23a1f2c7544f5cd31f59e7d7b0a35b@linea.org.br'], 
            'givenName': ['Glauber'], 
            'email': ['glauber.costa@linea.org.br'], 
            'cn': ['Glauber Vila Verde'], 
            'eduPersonEntitlement': ['urn:mace:rediris.es:entitlement:wiki:tfemc2'], 
            'sn': ['Vila Verde'], 
            'o': ['LINEA - Laboratorio Interinstitucional de e-Astronomia'], 
            'schacHomeOrganization': ['https://idp01.linea.org.br/idp/shibboleth'], 
            'uid': ['glauber.costa'], 
            'member': ['public', 'lsst', 'des', 'sdss', 'ton', 'itteam', 'loginapl', 'srvlogin', 'developers', 'hpeapolo', 'srvnodesprod']
            }, 
        'name_id': '<saml2.saml.NameID object at 0x7fb450a24ec0>',
        'came_from': '/', 
        'issuer': 'https://satosa-dev.linea.org.br/linea_saml_mirror/proxy/aHR0cHM6Ly9jaWxvZ29uLm9yZw==', 
        'not_on_or_after': 1749491076, 
        'authn_info': [
            ('default-LoA', ['https://cilogon.org/authorize'], '2025-06-09T17:29:36Z')
            ], 
        'session_index': 'id-rSjL0knPWfTUESDoM'}


    username = backend.get_user_identifier(attributes)
    print(f"Identificador do usuário: {username}")

 
    try:
        autorizado = backend.is_authorized(attributes, attribute_mapping, idp_entityid, assertion_info)
        print("Usuário autorizado?", autorizado)
    except Exception as e:
        print("Erro ao executar is_authorized:", e)

    try:
        userlookup, user_value = backend._extract_user_identifier_params(session_info, attributes, attribute_mapping)
        print(f"Usuário lookup: {userlookup} Usuario Value: {user_value}")
    except Exception as e:
        print("Erro ao executar extract_user:", e)


    # # Teste com falha simulada no COmanage
    # print("\n--- Teste com falha no COmanage ---")
    # backend.comanage.get_ldap_uid = MagicMock(side_effect=Exception("Simulated LDAP error"))
    # autorizado = backend.is_authorized(attributes, attribute_mapping, idp_entityid, assertion_info)
    # print("Usuário autorizado?", autorizado)


if __name__ == '__main__':
    main()