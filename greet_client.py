import greet_pb2_grpc
import greet_pb2
import time
import grpc

def run():
    # 1. create channel
    with grpc.insecure_channel('localhost:50051') as channel:
        # create client
        stub = greet_pb2_grpc.GreetingStub(channel)
        print("1. SayHello - Unary")
        rpc_call = input("Which rpc would you like to make: ")

        if rpc_call == "1":
            hello_request = greet_pb2.HelloRequest(name = "YouTube")
            hello_reply = stub.SayHello(hello_request)
            print("SayHello Response Received:")
            print(hello_reply)

if __name__ == "__main__":
    run()