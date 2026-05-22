/**
 * @fileoverview Core module for processing incoming data streams within the real-time fabric.
 * Handles data validation, transformation, and routing to distribution channels.
 *
 * This module ensures data integrity and prepares payloads for efficient real-time delivery.
 */

class DataStreamProcessor {
    constructor(config) {
        if (!config || !config.schemaValidator || !config.transformationEngine || !config.router) {
            throw new Error('DataStreamProcessor requires schemaValidator, transformationEngine, and router configurations.');
        }
        this.schemaValidator = config.schemaValidator;
        this.transformationEngine = config.transformationEngine;
        this.router = config.router;
        this.processingMetrics = {
            totalProcessed: 0,
            invalidPayloads: 0,
            transformationErrors: 0,
            routingFailures: 0
        };
        console.log('DataStreamProcessor initialized with robust data handling capabilities.');
    }

    /**
     * Ingests a raw data payload, processes it through validation and transformation,
     * and then routes it for distribution.
     * @param {object} rawPayload - The raw data object received from an upstream source.
     * @param {string} streamIdentifier - An identifier for the data stream (e.g., 'sensor_data', 'user_events').
     * @returns {Promise<boolean>} True if processing and routing were successful, false otherwise.
     */
    async process(rawPayload, streamIdentifier) {
        this.processingMetrics.totalProcessed++;
        try {
            // Step 1: Validate the incoming payload against a defined schema.
            const validationResult = this.schemaValidator.validate(rawPayload, streamIdentifier);
            if (!validationResult.isValid) {
                this.processingMetrics.invalidPayloads++;
                console.warn(`[${streamIdentifier}] Invalid payload received: ${validationResult.errors.join(', ')}`);
                return false;
            }

            // Step 2: Apply necessary transformations to normalize and enrich the data.
            let transformedPayload;
            try {
                transformedPayload = await this.transformationEngine.apply(rawPayload, streamIdentifier);
            } catch (transformError) {
                this.processingMetrics.transformationErrors++;
                console.error(`[${streamIdentifier}] Transformation failed: ${transformError.message}`);
                return false;
            }

            // Step 3: Route the processed payload to the appropriate real-time channels.
            const routingSuccess = await this.router.route(transformedPayload, streamIdentifier);
            if (!routingSuccess) {
                this.processingMetrics.routingFailures++;
                console.error(`[${streamIdentifier}] Failed to route transformed payload.`);
                return false;
            }

            return true; // Successfully processed and routed.

        } catch (generalError) {
            console.error(`[${streamIdentifier}] Unhandled error during stream processing: ${generalError.message}`);
            return false;
        }
    }

    /**
     * Retrieves current operational metrics for the data processor.
     * @returns {object} An object containing various processing statistics.
     */
    getMetrics() {
        return { ...this.processingMetrics };
    }
}

module.exports = DataStreamProcessor;