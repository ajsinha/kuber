/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Kafka Request/Response Test Client (v1.7.5) - C#/.NET
 * 
 * Tests the Kuber Request/Response messaging feature via Apache Kafka.
 * Publishes cache operation requests and subscribes to responses.
 *
 * Prerequisites:
 * - Kafka running on localhost:9092
 * - Kuber server running with messaging enabled
 * - Install NuGet package: Confluent.Kafka
 *     dotnet add package Confluent.Kafka
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Kuber.Client.Examples
{
    /// <summary>
    /// Test client for Kuber's Kafka-based Request/Response messaging.
    /// 
    /// Demonstrates how to:
    /// - Publish cache operation requests to Kafka
    /// - Subscribe to and receive responses
    /// - Correlate requests with responses using message IDs
    /// </summary>
    public class KafkaRequestResponseTest
    {
        // Configuration
        private const string BootstrapServers = "localhost:9092";
        private const string RequestTopic = "ccs_cache_request";
        private const string ResponseTopic = "ccs_cache_response";
        
        // Your Kuber API key (replace with actual key)
        private const string ApiKey = "kub_your_api_key_here";
        
        // Test region
        private const string TestRegion = "default";
        
        // Kafka clients
        private IProducer<string, string>? _producer;
        private IConsumer<string, string>? _consumer;
        
        // State
        private readonly CancellationTokenSource _cts = new();
        private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonNode>> _pendingRequests = new();
        private readonly List<JsonNode> _receivedResponses = new();
        private Task? _consumerTask;
        
        /// <summary>
        /// Main entry point.
        /// </summary>
        public static async Task Main(string[] args)
        {
            Console.WriteLine("╔════════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║  Kuber Kafka Request/Response Test Client (C#/.NET)                ║");
            Console.WriteLine("║  Tests cache operations via Kafka messaging                        ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();
            
            var test = new KafkaRequestResponseTest();
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
                // Initialize clients
                InitProducer();
                InitConsumer();
                
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
                await ShutdownAsync();
                PrintSummary();
            }
        }
        
        /// <summary>
        /// Initialize Kafka producer.
        /// </summary>
        private void InitProducer()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = BootstrapServers,
                Acks = Acks.All,
                MessageSendMaxRetries = 3
            };
            
            _producer = new ProducerBuilder<string, string>(config).Build();
            Console.WriteLine($"✓ Producer connected to {BootstrapServers}");
        }
        
        /// <summary>
        /// Initialize Kafka consumer.
        /// </summary>
        private void InitConsumer()
        {
            var groupId = $"kuber-test-client-{Guid.NewGuid().ToString()[..8]}";
            
            var config = new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true
            };
            
            _consumer = new ConsumerBuilder<string, string>(config).Build();
            _consumer.Subscribe(ResponseTopic);
            
            Console.WriteLine($"✓ Consumer connected (group: {groupId})");
            Console.WriteLine($"  Subscribed to: {ResponseTopic}");
        }
        
        /// <summary>
        /// Start background thread to listen for responses.
        /// </summary>
        private void StartResponseListener()
        {
            _consumerTask = Task.Run(() =>
            {
                Console.WriteLine("Response listener started");
                
                while (!_cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var result = _consumer!.Consume(TimeSpan.FromMilliseconds(500));
                        
                        if (result == null) continue;
                        
                        try
                        {
                            var response = JsonNode.Parse(result.Message.Value);
                            if (response == null) continue;
                            
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
                        catch (Exception ex)
                        {
                            Console.Error.WriteLine($"Error parsing response: {ex.Message}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        if (!_cts.Token.IsCancellationRequested)
                        {
                            Console.Error.WriteLine($"Consumer error: {ex.Message}");
                        }
                    }
                }
                
                Console.WriteLine("Response listener stopped");
            });
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
        /// Publish a request to Kafka.
        /// </summary>
        private async Task<TaskCompletionSource<JsonNode>> PublishRequestAsync(JsonObject request)
        {
            var messageId = request["message_id"]!.GetValue<string>();
            var requestJson = request.ToJsonString();
            
            var tcs = new TaskCompletionSource<JsonNode>();
            _pendingRequests[messageId] = tcs;
            
            try
            {
                var result = await _producer!.ProduceAsync(RequestTopic, 
                    new Message<string, string> { Key = messageId, Value = requestJson });
                
                Console.WriteLine();
                Console.WriteLine("📤 REQUEST SENT");
                Console.WriteLine($"   Message ID: {messageId}");
                Console.WriteLine($"   Operation: {request["operation"]}");
                Console.WriteLine($"   Region: {request["region"]}");
                Console.WriteLine($"   Key: {request["key"]}");
                Console.WriteLine($"   Partition: {result.Partition.Value}, Offset: {result.Offset.Value}");
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
                "kafka-test:user:1001",
                JsonSerializer.Serialize(new { name = "John Doe", email = "john@example.com", timestamp = DateTime.UtcNow }),
                3600);
            await PublishRequestAsync(setRequest);
            await Task.Delay(1000);
            
            Console.WriteLine("\n--- Test 2: GET Operation ---");
            var getRequest = CreateRequest("GET", TestRegion, "kafka-test:user:1001");
            await PublishRequestAsync(getRequest);
            await Task.Delay(1000);
            
            Console.WriteLine("\n--- Test 3: EXISTS Operation ---");
            var existsRequest = CreateRequest("EXISTS", TestRegion, "kafka-test:user:1001");
            await PublishRequestAsync(existsRequest);
            await Task.Delay(1000);
            
            Console.WriteLine("\n--- Test 4: SET Another Key ---");
            var setRequest2 = CreateRequest("SET", TestRegion,
                "kafka-test:user:1002",
                JsonSerializer.Serialize(new { name = "Jane Smith", role = "admin" }));
            await PublishRequestAsync(setRequest2);
            await Task.Delay(1000);
            
            Console.WriteLine("\n--- Test 5: KEYS Operation ---");
            var keysRequest = CreateRequest("KEYS", TestRegion, "kafka-test:*");
            await PublishRequestAsync(keysRequest);
            await Task.Delay(1000);
            
            Console.WriteLine("\n--- Test 6: DELETE Operation ---");
            var deleteRequest = CreateRequest("DELETE", TestRegion, "kafka-test:user:1002");
            await PublishRequestAsync(deleteRequest);
            await Task.Delay(1000);
            
            Console.WriteLine("\n--- Test 7: GET Non-existent Key ---");
            var getMissRequest = CreateRequest("GET", TestRegion, "kafka-test:nonexistent:key");
            await PublishRequestAsync(getMissRequest);
            await Task.Delay(1000);
            
            _producer!.Flush(TimeSpan.FromSeconds(5));
            Console.WriteLine("\n✓ All requests sent and flushed");
        }
        
        /// <summary>
        /// Shutdown clients.
        /// </summary>
        private async Task ShutdownAsync()
        {
            Console.WriteLine("\nShutting down...");
            _cts.Cancel();
            
            if (_consumerTask != null)
            {
                try
                {
                    await _consumerTask.WaitAsync(TimeSpan.FromSeconds(5));
                }
                catch (TimeoutException) { }
            }
            
            _producer?.Dispose();
            _consumer?.Close();
            _consumer?.Dispose();
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
