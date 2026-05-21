/**
 * @file src/websocket/WebSocketStreamProcessor.js
 * @description Core module for handling WebSocket connections, message parsing, and routing within the data fabric.
 * @author [Your Name/Org]
 * @license MIT
 */

const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const { validateSchema, applyTransformations } = require('../core/DataIntegrityService');
const { publishToTopic, subscribeToTopic, unsubscribeFromTopic } = require('../core/MessageBroker');
const logger = require('../utils/Logger');
const config = require('../config/fabricConfig');

/**
 * Manages individual client WebSocket connections and their data streams.
 */
class WebSocketClient {
  constructor(ws, id) {
    this.ws = ws;
    this.id = id;
    this.subscriptions = new Set();
    this.lastActivity = Date.now();
    this.initHandlers();
    logger.info(`Client ${this.id} connected from ${ws._socket.remoteAddress}`);
  }

  initHandlers() {
    this.ws.on('message', this.handleMessage.bind(this));
    this.ws.on('close', this.handleClose.bind(this));
    this.ws.on('error', this.handleError.bind(this));
  }

  /**
   * Processes incoming messages from a client.
   * @param {string} message - The raw message string received from the client.
   */
  async handleMessage(message) {
    this.lastActivity = Date.now();
    try {
      const parsedMessage = JSON.parse(message);
      const { type, payload, topic, schemaId, transformId } = parsedMessage;

      if (!type) {
        throw new Error('Message type is required.');
      }

      switch (type) {
        case 'PUBLISH':
          if (!topic || !payload) {
            throw new Error('PUBLISH messages require a topic and payload.');
          }
          await this.processPublish(topic, payload, schemaId, transformId);
          break;
        case 'SUBSCRIBE':
          if (!topic) {
            throw new Error('SUBSCRIBE messages require a topic.');
          }
          this.processSubscribe(topic);
          break;
        case 'UNSUBSCRIBE':
          if (!topic) {
            throw new Error('UNSUBSCRIBE messages require a topic.');
          }
          this.processUnsubscribe(topic);
          break;
        case 'PING':
          this.send('PONG', { timestamp: Date.now() });
          break;
        default:
          throw new Error(`Unknown message type: ${type}`);
      }
    } catch (error) {
      logger.warn(`Client ${this.id} message error: ${error.message}. Message: ${message}`);
      this.send('ERROR', { message: error.message });
    }
  }

  /**
   * Publishes data to a specified topic after validation and transformation.
   * @param {string} topic - The topic to publish to.
   * @param {object} data - The data payload.
   * @param {string} [schemaId] - Optional schema ID for validation.
   * @param {string} [transformId] - Optional transformation ID to apply.
   */
  async processPublish(topic, data, schemaId, transformId) {
    try {
      let processedData = data;
      if (schemaId) {
        validateSchema(schemaId, processedData);
      }
      if (transformId) {
        processedData = await applyTransformations(transformId, processedData);
      }
      await publishToTopic(topic, processedData);
      logger.debug(`Client ${this.id} published to topic '${topic}'`);
      this.send('ACK', { topic, status: 'success' });
    } catch (error) {
      logger.error(`Failed to publish from client ${this.id} to topic '${topic}': ${error.message}`);
      this.send('NACK', { topic, status: 'error', message: error.message });
    }
  }

  /**
   * Subscribes the client to a topic.
   * @param {string} topic - The topic to subscribe to.
   */
  processSubscribe(topic) {
    if (this.subscriptions.has(topic)) {
      logger.debug(`Client ${this.id} already subscribed to topic '${topic}'`);
      this.send('ACK', { topic, status: 'already_subscribed' });
      return;
    }
    subscribeToTopic(topic, this.id, this.receiveData.bind(this));
    this.subscriptions.add(topic);
    logger.info(`Client ${this.id} subscribed to topic '${topic}'`);
    this.send('ACK', { topic, status: 'success' });
  }

  /**
   * Unsubscribes the client from a topic.
   * @param {string} topic - The topic to unsubscribe from.
   */
  processUnsubscribe(topic) {
    if (!this.subscriptions.has(topic)) {
      logger.debug(`Client ${this.id} not subscribed to topic '${topic}'`);
      this.send('ACK', { topic, status: 'not_subscribed' });
      return;
    }
    unsubscribeFromTopic(topic, this.id);
    this.subscriptions.delete(topic);
    logger.info(`Client ${this.id} unsubscribed from topic '${topic}'`);
    this.send('ACK', { topic, status: 'success' });
  }

  /**
   * Sends data to the client.
   * @param {string} type - The message type.
   * @param {object} payload - The data payload.
   * @param {string} [topic] - Optional topic context.
   */
  send(type, payload, topic = null) {
    if (this.ws.readyState === WebSocket.OPEN) {
      try {
        const message = JSON.stringify({ type, payload, topic, timestamp: Date.now() });
        this.ws.send(message);
      } catch (error) {
        logger.error(`Failed to send message to client ${this.id}: ${error.message}`);
      }
    }
  }

  /**
   * Callback for receiving data from the message broker.
   * @param {string} topic - The topic the data originated from.
   * @param {object} data - The data payload.
   */
  receiveData(topic, data) {
    this.send('DATA', data, topic);
  }

  /**
   * Handles client disconnection.
   */
  handleClose() {
    logger.info(`Client ${this.id} disconnected.`);
    this.subscriptions.forEach(topic => {
      unsubscribeFromTopic(topic, this.id);
    });
    WebSocketStreamProcessor.removeClient(this.id);
  }

  /**
   * Handles WebSocket errors.
   * @param {Error} error - The error object.
   */
  handleError(error) {
    logger.error(`Client ${this.id} WebSocket error: ${error.message}`);
    this.ws.close(1011, 'Internal Error');
  }

  /**
   * Checks if the client is still active.
   * @returns {boolean}
   */
  isAlive() {
    return this.ws.readyState === WebSocket.OPEN && (Date.now() - this.lastActivity < config.websocket.clientTimeout);
  }
}

/**
 * Main WebSocket server class for the data fabric.
 */
class WebSocketStreamProcessor {
  constructor(server) {
    this.wss = new WebSocket.Server({ server });
    this.clients = new Map(); // Map<clientId, WebSocketClient>
    this.setupHeartbeat();
    this.wss.on('connection', this.handleConnection.bind(this));
    this.wss.on('error', this.handleServerError.bind(this));
    logger.info('WebSocketStreamProcessor initialized.');
  }

  /**
   * Handles new incoming WebSocket connections.
   * @param {WebSocket} ws - The raw WebSocket connection.
   */
  handleConnection(ws) {
    const clientId = uuidv4();
    const client = new WebSocketClient(ws, clientId);
    this.clients.set(clientId, client);
  }

  /**
   * Sets up a periodic heartbeat to detect inactive clients.
   */
  setupHeartbeat() {
    setInterval(() => {
      this.clients.forEach(client => {
        if (!client.isAlive()) {
          logger.warn(`Terminating inactive client ${client.id}`);
          client.ws.terminate();
        } else {
          // Optionally send a PING to prompt client activity
          // client.send('PING', { serverTimestamp: Date.now() });
        }
      });
    }, config.websocket.heartbeatInterval);
  }

  /**
   * Removes a client from the active connections.
   * @param {string} clientId - The ID of the client to remove.
   */
  static removeClient(clientId) {
    // Access the instance via a static property or pass 'this' if appropriate
    // For simplicity, we'll assume a singleton pattern or direct access is handled externally.
    // In a real scenario, this would likely be an instance method or use a global manager.
    if (this.instance && this.instance.clients.has(clientId)) {
      this.instance.clients.delete(clientId);
      logger.debug(`Client ${clientId} removed from processor.`);
    }
  }

  /**
   * Handles server-level WebSocket errors.
   * @param {Error} error - The error object.
   */
  handleServerError(error) {
    logger.error(`WebSocket server error: ${error.message}`);
    // Potentially restart the server or notify monitoring systems
  }

  /**
   * Public method to broadcast a message to all subscribed clients.
   * This could be used by internal services to push updates.
   * @param {string} topic - The topic to broadcast on.
   * @param {object} data - The data payload.
   */
  broadcast(topic, data) {
    this.clients.forEach(client => {
      if (client.subscriptions.has(topic)) {
        client.receiveData(topic, data);
      }
    });
    logger.debug(`Broadcasted data to topic '${topic}' to ${this.clients.size} clients.`);
  }
}

// Singleton instance for easy access from other modules
let instance = null;

module.exports = (server) => {
  if (!instance) {
    instance = new WebSocketStreamProcessor(server);
    WebSocketStreamProcessor.instance = instance; // Expose instance for static method
  }
  return instance;
};
