from rest_framework_simplejwt.tokens import UntypedToken
from rest_framework_simplejwt.exceptions import InvalidToken, TokenError
from logging_module import logger

def get_user_id_from_token(request):
    try:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            try:
                decoded_token = UntypedToken(token)  # Automatically validates the token
            except (InvalidToken, TokenError) as e:
                return {"error": "Invalid token", "status": "error_401"}
            return {"user_id": decoded_token.payload["user_id"], "status": "success"}
        logger.error("No token provided")
        return {"error": "No token provided", "status": "error_401"}
    except Exception as e:
        logger.error(f"Error in get_user_id_from_token: {str(e)}")
        return {"error": str(e), "status": "error_500"}