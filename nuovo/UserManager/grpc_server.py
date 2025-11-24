import grpc
from concurrent import futures
import auth_pb2
import auth_pb2_grpc
import mysql.connector, os

def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )

class AuthServicer(auth_pb2_grpc.AuthServiceServicer):
    def CheckUser(self, request):
        email = request.email
        
        db = get_connection()
        cursor = db.cursor()
        cursor.execute("SELECT email FROM utenti WHERE email = %s", (email,))
        exists = cursor.fetchone() is not None
        
        cursor.close()
        db.close()

        return auth_pb2.UserResponse(exists=exists)

def service():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    auth_pb2_grpc.add_AuthServiceServicer_to_server(AuthServicer(), server)
    server.add_insecure_port('[::]:6000')
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    service()