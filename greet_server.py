from concurrent import futures
import time

import grpc
import greet_pb2
import greet_pb2_grpc
import logging
import asyncio

class GreetingServicer(greet_pb2_grpc.GreetingServicer):
    def SayHello(self, request, context):
        print("SayHello Request Made:")
        print(request)
        hello_reply = greet_pb2.HelloReply()
        hello_reply.message = f"Hello from python server: {request.name}"

        return hello_reply

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    greet_pb2_grpc.add_GreetingServicer_to_server(GreetingServicer(), server)
    server.add_insecure_port("localhost:50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()