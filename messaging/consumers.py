from channels.generic.websocket import AsyncJsonWebsocketConsumer
from channels.db import database_sync_to_async
from django.contrib.auth import get_user_model
import jwt
from django.conf import settings
from django_redis import get_redis_connection
from urllib.parse import parse_qs

User = get_user_model()


class ConversationConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        await self.accept()
        user = await self.get_user_from_jwt()
        if user.is_authenticated:
            _self_channel = f"{user.id}-SELF"
            await self.channel_layer.group_add(_self_channel, self.channel_name)
            self.scope["user"] = user
            await self.add_user_to_online_list(user)
        else:
            await self.close()

        # send welcome message to user
        await self.send_json({"message": "Welcome to the chat!"})

    async def users_online(self, event):
        message = f"{event["data"]['display_name']} is online."
        await self.send_json(
            {"message": message, "user_id": event["data"]["user_id"], "type": "online"}
        )

    async def users_offline(self, event):
        message = f"{event['data']['display_name']} is offline."
        await self.send_json(
            {"message": message, "user_id": event["data"]["user_id"], "type": "offline"}
        )

    async def disconnect(self, close_code):
        if self.scope["user"].is_authenticated:
            await self.remove_user_from_online_list(self.scope["user"])

    async def send_notification(self, event):
        await self.send_json(event["message"])

    async def receive_json(self, content):
        content["sender"] = self.scope["user"].id
        if content["type"] == "text":
            await self.channel_layer.group_send(
                f"{content['receiver']}-SELF",
                {
                    "type": "peer.text",
                    "data": content,
                },
            )
        elif content["type"] == "site_update":
            await self.channel_layer.group_send(
                f"{self.scope['user'].id}-SELF",
                {
                    "type": "site_update",
                    "site_name": "",
                    "site_id": "",
                    "new_updates": "",
                    "time": "",
                },
            )
        elif content["type"] == "new_records":
            await self.channel_layer.group_send(
                f"{self.scope['user'].id}-SELF",
                {
                    "type": "new_records",
                    "vendor_name": "vendor_name",
                    "new_updates": 0,
                },
            )
        else:
            await self.send_json(
                {"message": "No Event Type Found. Please ensure correct event type."}
            )

    async def peer_text(self, event):
        await self.send_json(event)

    async def site_update(self, event):
        await self.send_json(event)

    async def new_records(self, event):
        await self.send_json(event)

    @database_sync_to_async
    def get_user_from_jwt(self):
        try:
            query_string = self.scope.get("query_string", b"").decode("utf-8")
            query_params = parse_qs(query_string)
            token = query_params.get("token", [None])[0]
            if token:
                decoded_token = jwt.decode(
                    token, settings.SECRET_KEY, algorithms=["HS256"]
                )
                user_id = decoded_token.get("user_id")
                user = User.objects.get(id=user_id)
                return user

            authorization_header = None
            for name, value in self.scope["headers"]:
                if name == b"authorization":
                    authorization_header = value.decode("utf-8")
                    break

            if authorization_header and authorization_header.startswith("Bearer "):
                token = authorization_header.split("Bearer ")[1]
                decoded_token = jwt.decode(
                    token, settings.SECRET_KEY, algorithms=["HS256"]
                )
                user_id = decoded_token.get("user_id")
                user = User.objects.get(id=user_id)
                return user

            else:
                raise ValueError("Invalid or missing Authorization header")
        except Exception as e:
            raise ValueError("Invalid or missing Authorization header")

    @database_sync_to_async
    def add_user_to_online_list(self, user):
        redis_conn = get_redis_connection("default")
        redis_conn.sadd("online_users", user.id)

    @database_sync_to_async
    def remove_user_from_online_list(self, user):
        redis_conn = get_redis_connection("default")
        redis_conn.srem("online_users", user.id)
