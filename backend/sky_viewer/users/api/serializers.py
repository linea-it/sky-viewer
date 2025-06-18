from rest_framework import serializers

from sky_viewer.users.models import User


class UserSerializer(serializers.ModelSerializer[User]):
    groups = serializers.SerializerMethodField()
    class Meta:
        model = User
        fields = ["username", "name", "groups"]

        extra_kwargs = {
            "url": {"view_name": "api:user-detail", "lookup_field": "username"},
        }

    def get_groups(self, obj):
        return [group.name for group in obj.groups.all()]