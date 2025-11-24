import grpc
import auth_pb2, auth_pb2_grpc

def check_user(email):
    channel = grpc.insecure_channel("service_b:6000")
    stub = auth_pb2_grpc.AuthServiceStub(channel)

    response = stub.CheckUser(auth_pb2.UserRequest(email=email))
    return response.exists