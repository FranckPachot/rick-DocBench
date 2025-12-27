/**
 * MongoDB adapter implementation for DocBench.
 *
 * <p>This package provides the MongoDB database adapter which instruments
 * BSON operations for overhead decomposition analysis. Key classes:
 *
 * <ul>
 *   <li>{@link com.docbench.adapter.mongodb.MongoDBAdapter} - Main adapter implementation</li>
 *   <li>{@link com.docbench.adapter.mongodb.MongoDBInstrumentedConnection} - Connection wrapper with timing hooks</li>
 *   <li>{@link com.docbench.adapter.mongodb.BsonTimingInterceptor} - CommandListener for timing capture</li>
 * </ul>
 *
 * <h2>BSON Traversal Characteristics</h2>
 *
 * <p>MongoDB uses BSON (Binary JSON) which has these traversal characteristics:
 * <ul>
 *   <li>O(n) sequential field-name scanning at each document level</li>
 *   <li>Length-prefixed elements allow sub-document skipping</li>
 *   <li>Field position impacts access time</li>
 *   <li>Both server and client must traverse during operations</li>
 * </ul>
 *
 * <h2>Metrics Captured</h2>
 *
 * <ul>
 *   <li>mongodb.client_round_trip - Total client-side operation time</li>
 *   <li>mongodb.server_execution - Server-reported execution time</li>
 *   <li>mongodb.overhead - Difference (network, serialization, etc.)</li>
 *   <li>mongodb.[command].* - Per-command-type metrics</li>
 * </ul>
 */
package com.docbench.adapter.mongodb;
