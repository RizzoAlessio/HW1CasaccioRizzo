import grpc
from proto import auth_pb2, auth_pb2_grpc

def check_user(email):
    try:
        chan = grpc.insecure_channel("user_grpc:5004")
        stub = auth_pb2_grpc.AuthServiceStub(chan)
        req = auth_pb2.UserRequest(email=email)
        response = stub.CheckUser(req)
        return {"exists": response.exists, "error": None}
    except Exception as e:
        return {"exists": None, "error": str(e)}