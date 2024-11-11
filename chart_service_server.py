import grpc
from concurrent import futures
from chart_service_pb2_grpc import add_ChartServiceServicer_to_server, ChartServiceServicer
from chart_service_pb2 import SubscribeResponse, Candlestick
import time  

class ChartService(ChartServiceServicer):
    def Subscribe(self, request, context):
    
        for i in range(2):  # Example loop to send 2  dummy candlesticks
            candle = Candlestick(
                timestamp_msec=int(time.time() * 1000),  # Current time in milliseconds
                open=100.0 + i,
                high=105.0 + i,
                low=95.0 + i,
                close=102.0 + i
            )
            response = SubscribeResponse(symbol=request.symbol_list[0], bar=candle)
            yield response
            time.sleep(60)  # Simulate a delay for 1 minute

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_ChartServiceServicer_to_server(ChartService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server is running on port 50051...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()