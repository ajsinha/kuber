/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber ActiveMQ Request/Response Test Client (v1.7.5) - C#/.NET
 * 
 * Tests the Kuber Request/Response messaging feature via Apache ActiveMQ.
 * Publishes cache operation requests and subscribes to responses.
 *
 * Prerequisites:
 * - ActiveMQ running on localhost:61616
 * - Kuber server running with messaging enabled
 * - Install NuGet packages:
 *     dotnet add package Apache.NMS
 *     dotnet add package Apache.NMS.ActiveMQ
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.ActiveMQ;

namespace Kuber.Client.Examples
{
    /// <summary>
    /// Test client for Kuber's ActiveMQ-based Request/Response messaging.
    /// 
    /// Demonstrates how to:
    /// - Publish cache operation requests to ActiveMQ queues
    /// - Subscribe to and receive responses
    /// - Correlate requests with responses using message IDs
    /// </summary>
    public class ActiveMqRequestResponseTest
    {
        // Configuration
        private const string BrokerUri = "activemq:tcp://localhost:61616";
        private const string Username = "admin";
        private const string Password = "admin";
        private const string RequestQueue = "ccs_cache_request";
        private const string ResponseQueue = "ccs_cache_response";
        
        // Your Kuber API key (replace with actual key)
        private const string ApiKey = "kub_your_api_key_here";
        
        // Test region
        private const string TestRegion = "default";
        
        // NMS objects
        private IConnectionFactory? _factory;
        private IConnection? _connection;
        private ISession? _session;
        private IMessageProducer? _producer;
        private IMessageConsumer? _consumer;
        
        // State
        private readonly CancellationTokenSource _cts = new();
        private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonNode>> _pendingRequests = new();
        private readonly List<JsonNode> _receivedResponses = new();
        
        /// <summary>
        /// Main entry point.
        /// </summary>
        public static async Task Main(string[] args)
        {
            Console.WriteLine("╔════════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║  Kuber ActiveMQ Request/Response Test Client (C#/.NET)             ║");
            Console.WriteLine("║  Tests cache operations via ActiveMQ messaging                     ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();
            
            var test = new ActiveMqRequestResponseTest();
            try
            {
                await test.RunAsync();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error: {ex.Message}");
                Console.Error.WriteLine(ex.StackTrace);
            }
        }
        
        /// <summary>
        /// Run the test suite.
        /// </summary>
        public async Task RunAsync()
        {
            try
            {
                // Initialize NMS connection
                InitConnection();
                
                // Start response listener
                StartResponseListener();
                
                // Give consumer time to connect
                await Task.Delay(2000);
                
                // Run tests
                await RunTestSuiteAsync();
                
                // Wait for responses
                Console.WriteLine("\nWaiting for responses (10 seconds)...");
                await Task.Delay(10000);
            }
            finally
            {
                Shutdown();
                PrintSummary();
            }
        }
        
        /// <summary>
        /// Initialize NMS connection to ActiveMQ.
        /// </summary>
        private void InitConnection()
        {
            Console.WriteLine($"Connecting to ActiveMQ at {BrokerUri}...");
            
            _factory = new ConnectionFactory(BrokerUri);
            _connection = _factory.CreateConnection(Username, Password);
            _connection.Start();
            
            _session = _connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            
            // Create producer for request queue
            var requestDest = _session.GetQueue(RequestQueue);
            _producer = _session.CreateProducer(requestDest);
            _producer.DeliveryMode = MsgDeliveryMode.Persistent;
            
            // Create consumer for response queue
            var responseDest = _session.GetQueue(ResponseQueue);
            _consumer = _session.CreateConsumer(responseDest);
            
            Console.WriteLine("✓ Connected to ActiveMQ");
            Console.WriteLine($"  Request queue: {RequestQueue}");
            Console.WriteLine($"  Response queue: {ResponseQueue}");
        }
        
        /// <summary>
        /// Start listening for responses.
        /// </summary>
        private void StartResponseListener()
        {
            _consumer!.Listener += message =>
            {
                try
                {
                    if (message is ITextMessage textMessage)
                    {
                        var responseText = textMessage.Text;
                        var response = JsonNode.Parse(responseText);
                        if (response == null) return;
                        
                        // Kuber response structure is nested:
                        // {
                        //   "request_receive_timestamp": "...",
                        //   "response_time": "...",
                        //   "processing_time_ms": 123,
                        //   "request": { original request with message_id, operation, etc. },
                        //   "response": { "success": true/false, "result": ..., "error": "..." }
                        // }
                        var requestNode = response["request"];
                        var responseNode = response["response"];
                        
                        var messageId = requestNode?["message_id"]?.GetValue<string>() ?? "unknown";
                        var success = responseNode?["success"]?.GetValue<bool>() ?? false;
                        var operation = requestNode?["operation"]?.GetValue<string>() ?? "unknown";
                        var processingTime = response["processing_time_ms"]?.GetValue<long>() ?? 0;
                        
                        Console.WriteLine();
                        Console.WriteLine("════════════════════════════════════════════════════════════");
                        Console.WriteLine("📥 RESPONSE RECEIVED");
                        Console.WriteLine($"   Message ID: {messageId}");
                        Console.WriteLine($"   Success: {success}");
                        Console.WriteLine($"   Operation: {operation}");
                        Console.WriteLine($"   Processing Time: {processingTime}ms");
                        
                        if (success)
                        {
                            var resultValue = responseNode?["result"]?.ToJsonString();
                            if (!string.IsNullOrEmpty(resultValue))
                            {
                                if (resultValue.Length > 100)
                                {
                                    Console.WriteLine($"   Result: {resultValue[..100]}...");
                                }
                                else
                                {
                                    Console.WriteLine($"   Result: {resultValue}");
                                }
                            }
                        }
                        else
                        {
                            var errorCode = responseNode?["error_code"]?.GetValue<string>() ?? "UNKNOWN";
                            var error = responseNode?["error"]?.GetValue<string>() ?? "Unknown error";
                            Console.WriteLine($"   Error Code: {errorCode}");
                            Console.WriteLine($"   Error: {error}");
                        }
                        Console.WriteLine("════════════════════════════════════════════════════════════");
                        
                        lock (_receivedResponses)
                        {
                            _receivedResponses.Add(response);
                        }
                        
                        // Complete pending task if exists
                        if (_pendingRequests.TryRemove(messageId, out var tcs))
                        {
                            tcs.SetResult(response);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Error processing response: {ex.Message}");
                }
            };
            
            Console.WriteLine("✓ Response listener started");
        }
        
        /// <summary>
        /// Create a request message.
        /// </summary>
        private JsonObject CreateRequest(string operation, string region, string key,
            string? value = null, int? ttl = null)
        {
            var messageId = $"req-{Guid.NewGuid().ToString()[..12]}";
            
            var request = new JsonObject
            {
                ["api_key"] = ApiKey,
                ["message_id"] = messageId,
                ["operation"] = operation.ToUpperInvariant(),
                ["region"] = region,
                ["key"] = key
            };
            
            if (!string.IsNullOrEmpty(value))
            {
                request["value"] = value;
            }
            
            if (ttl.HasValue)
            {
                request["ttl"] = ttl.Value;
            }
            
            return request;
        }
        
        /// <summary>
        /// Publish a request to ActiveMQ.
        /// </summary>
        private TaskCompletionSource<JsonNode> PublishRequest(JsonObject request)
        {
            var messageId = request["message_id"]!.GetValue<string>();
            var requestJson = request.ToJsonString();
            
            var tcs = new TaskCompletionSource<JsonNode>();
            _pendingRequests[messageId] = tcs;
            
            try
            {
                var message = _session!.CreateTextMessage(requestJson);
                message.NMSCorrelationID = messageId;
                _producer!.Send(message);
                
                Console.WriteLine();
                Console.WriteLine("📤 REQUEST SENT");
                Console.WriteLine($"   Message ID: {messageId}");
                Console.WriteLine($"   Operation: {request["operation"]}");
                Console.WriteLine($"   Region: {request["region"]}");
                Console.WriteLine($"   Key: {request["key"]}");
                Console.WriteLine($"   Queue: {RequestQueue}");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"✗ Failed to send: {ex.Message}");
                tcs.SetException(ex);
            }
            
            return tcs;
        }
        
        /// <summary>
        /// Run the test suite.
        /// </summary>
        private async Task RunTestSuiteAsync()
        {
            Console.WriteLine("\n--- Test 1: SET Operation ---");
            var setRequest = CreateRequest("SET", TestRegion,
                "activemq-test:user:1001",
                JsonSerializer.Serialize(new { name = "John Doe", email = "john@example.com", timestamp = DateTime.UtcNow }),
                3600);
            PublishRequest(setRequest);
            await Task.Delay(1000);
            
            Console.WriteLine("\n--- Test 2: GET Operation ---");
            var getRequest = CreateRequest("GET", TestRegion, "activemq-test:user:1001");
            PublishRequest(getRequest);
            await Task.Delay(1000);
            
            Console.WriteLine("\n--- Test 3: EXISTS Operation ---");
            var existsRequest = CreateRequest("EXISTS", TestRegion, "activemq-test:user:1001");
            PublishRequest(existsRequest);
            await Task.Delay(1000);
            
            Console.WriteLine("\n--- Test 4: SET Another Key ---");
            var setRequest2 = CreateRequest("SET", TestRegion,
                "activemq-test:user:1002",
                JsonSerializer.Serialize(new { name = "Jane Smith", role = "admin" }));
            PublishRequest(setRequest2);
            await Task.Delay(1000);
            
            Console.WriteLine("\n--- Test 5: KEYS Operation ---");
            var keysRequest = CreateRequest("KEYS", TestRegion, "activemq-test:*");
            PublishRequest(keysRequest);
            await Task.Delay(1000);
            
            Console.WriteLine("\n--- Test 6: DELETE Operation ---");
            var deleteRequest = CreateRequest("DELETE", TestRegion, "activemq-test:user:1002");
            PublishRequest(deleteRequest);
            await Task.Delay(1000);
            
            Console.WriteLine("\n--- Test 7: GET Non-existent Key ---");
            var getMissRequest = CreateRequest("GET", TestRegion, "activemq-test:nonexistent:key");
            PublishRequest(getMissRequest);
            await Task.Delay(1000);
            
            Console.WriteLine("\n✓ All requests sent");
        }
        
        /// <summary>
        /// Shutdown connections.
        /// </summary>
        private void Shutdown()
        {
            Console.WriteLine("\nShutting down...");
            _cts.Cancel();
            
            try
            {
                _consumer?.Close();
                _producer?.Close();
                _session?.Close();
                _connection?.Close();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error closing connections: {ex.Message}");
            }
        }
        
        /// <summary>
        /// Print test summary.
        /// </summary>
        private void PrintSummary()
        {
            Console.WriteLine();
            Console.WriteLine("════════════════════════════════════════════════════════════");
            Console.WriteLine("TEST SUMMARY");
            Console.WriteLine("════════════════════════════════════════════════════════════");
            
            lock (_receivedResponses)
            {
                Console.WriteLine($"Total responses received: {_receivedResponses.Count}");
            }
            Console.WriteLine($"Pending requests (no response): {_pendingRequests.Count}");
            
            if (!_pendingRequests.IsEmpty)
            {
                Console.WriteLine("\nRequests without responses:");
                foreach (var messageId in _pendingRequests.Keys)
                {
                    Console.WriteLine($"  - {messageId}");
                }
            }
            
            int successCount = 0;
            lock (_receivedResponses)
            {
                foreach (var r in _receivedResponses)
                {
                    var responseNode = r["response"];
                    if (responseNode?["success"]?.GetValue<bool>() == true)
                    {
                        successCount++;
                    }
                }
            }
            
            var errorCount = _receivedResponses.Count - successCount;
            
            Console.WriteLine($"\nSuccessful operations: {successCount}");
            Console.WriteLine($"Failed operations: {errorCount}");
            Console.WriteLine("════════════════════════════════════════════════════════════");
        }
    }
}
