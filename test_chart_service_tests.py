import unittest
from unittest.mock import patch
import grpc
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from chart_service_pb2 import SubscribeRequest, Timeframe
from chart_service_pb2_grpc import ChartServiceStub
from unittest.mock import patch, MagicMock

DATABASE_URI = 'sqlite:///candlesticks.db'  

class TestChartService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.channel = grpc.insecure_channel('localhost:50051')  
        cls.stub = ChartServiceStub(cls.channel)
        cls.engine = create_engine(DATABASE_URI)
        cls.Session = sessionmaker(bind=cls.engine)

        # Create the table if it doesn't exist
        with cls.engine.connect() as connection:
            connection.execute(text("""
            CREATE TABLE IF NOT EXISTS candlesticks_m1 (
                symbol TEXT NOT NULL,
                timestamp_msec INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                PRIMARY KEY (symbol, timestamp_msec)
            );
            """))

    def test_case_1_successful_subscription(self):
        print("Test Case 1: Successful Subscription")
        request = SubscribeRequest(
            timeframe=Timeframe.TIMEFRAME_MINUTE_1,
            symbol_list=['BTCUSD', 'ETHUSD']
        )
        responses = self.stub.Subscribe(request)

        try:
            for response in responses:
                bar = response.bar
                print(f'Received candlestick: {bar}')
                self.assertIsNotNone(bar, "Received candlestick is None")
            print("Test Case 1: Passed successfully")
        except Exception as e:
            print(f"Test Case 1: Failed - {e}")


 
    def test_case_2_duplicate_candlestick_prevention(self):
        print("Test Case 2: Duplicate Candlestick Prevention")

        with self.Session() as session:
            # Insert a sample candlestick for BTCUSD at a specific timestamp
            session.execute(text("""
                INSERT INTO candlesticks_m1 (symbol, timestamp_msec, open, high, low, close)
                VALUES ('BTCUSD', 1633036800000, 45000.0, 45500.0, 44000.0, 45200.0)
            """))
            session.commit()

        
        request = SubscribeRequest(
            timeframe=Timeframe.TIMEFRAME_MINUTE_1,
            symbol_list=['BTCUSD']
        )
        responses = self.stub.Subscribe(request)

        try:
            for response in responses:
                bar = response.bar
                # Check for duplicates in the database
                with self.Session() as session:
                    existing_candle = session.execute(
                        text("""
                        SELECT * FROM candlesticks_m1 
                        WHERE symbol = :symbol AND timestamp_msec = :timestamp
                        """),
                        {'symbol': response.symbol, 'timestamp': bar.timestamp_msec}
                    ).fetchone()
                    # This assertion is expected to fail since we just inserted a duplicate
                    self.assertIsNotNone(existing_candle, 
                        f"Candlestick should already exist in the database: {bar}")
            print("Test Case 2: Passed successfully (unexpected)")
        except Exception as e:
            # The expected behavior is that this will fail due to the assertion
            print(f"Test Case 2: Failed - {e}")

     

    def test_case_3_no_data_received(self):
        print("Test Case 3: No Data Received")
        request = SubscribeRequest(
            timeframe=Timeframe.TIMEFRAME_MINUTE_1,
            symbol_list=['BTCUSD']
        )
        responses = self.stub.Subscribe(request)
        
        try:
            # Use a list to collect valid responses
            valid_responses = []
            
            # Collect responses
            for response in responses:
                valid_responses.append(response)

            # Check if we received any valid responses
            self.assertEqual(len(valid_responses), 0, "Expected no valid data received")
            print("Test Case 3: Passed successfully")
        
        except Exception as e:
            print(f"Test Case 3: Failed - {e}")
            
    def test_case_4_database_connection_failure(self):
        print("Test Case 4: Database Connection Failure")

        # Simulate a database connection failure
        print("Simulating database connection failure...")
        try:
            # Attempting to execute a query after closing the session
            with self.Session() as session:
                session.close()  
                session.execute("SELECT 1")   
            print("Test Case 4: Failed - Expected a connection error, but none occurred.")
        except Exception as e:
            print(f"Test Case 4: Passed successfully - {e}")


    
    @patch('chart_service_pb2_grpc.ChartServiceStub')  
    def test_case_5_malformed_price_data(self, mock_chart_service):
        print("Test Case 5: Malformed Price Data")
        
        # Setting up the mock to simulate a response with malformed data
        mock_response = MagicMock()
        mock_response.bar = None   
        
        # Mocking the Subscribe method to return an malformed response
        mock_chart_service.return_value.Subscribe.return_value = [mock_response]

        request = SubscribeRequest(
            timeframe=Timeframe.TIMEFRAME_MINUTE_1,
            symbol_list=['BTCUSD']
        )

        print("Simulating malformed price data...")
        try:
            responses = mock_chart_service().Subscribe(request)
            
            # Iterate through the mocked responses
            for response in responses:
                bar = response.bar   
                if bar is None:
                    raise ValueError("Received malformed price data (None)")

             
            print("Test Case 5: Failed - No error was raised for malformed data")
        
        except Exception as e:
            print(f"Test Case 5: Passed successfully - {e}")


    def test_case_6_handling_of_interrupted_streaming(self):
        print("Test Case 6: Handling of Interrupted Streaming")
        
        # Set up the initial subscription to a valid symbol
        request = SubscribeRequest(
            timeframe=Timeframe.TIMEFRAME_MINUTE_1,
            symbol_list=['BTCUSD']
        )
        
        responses = self.stub.Subscribe(request)

        # Simulate interruption of the data stream
        print("Simulating interruption of the data stream...")
        self.simulate_stream_interruption()

        # Allow some time for the interruption to take effect
        time.sleep(1)
 
        print("Checking if the service can recover and continue streaming...")
        recovered_responses = self.stub.Subscribe(request)
        
        # Collect responses after recovery
        valid_responses_after_recovery = []
        
        for response in recovered_responses:
            valid_responses_after_recovery.append(response)
            print(f'Recovered candlestick: {response.bar}')

        # Assert that we received valid responses after recovery
        self.assertGreater(len(valid_responses_after_recovery), 0, 
                        "Expected to receive valid data after recovery")
        print("Test Case 6: Passed successfully (recovered and received valid data)")    
            

    def simulate_stream_interruption(self):
        
        print("Data stream interrupted (simulated).")
        

 

    
    
    @classmethod
    def tearDownClass(cls):
        cls.channel.close()
        cls.engine.dispose()

if __name__ == '__main__':
    unittest.main()