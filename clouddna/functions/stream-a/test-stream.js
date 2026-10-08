const streamAFunc = require("./EventHubTrigger/index.js");

async function runTest() {
    console.log("=== Testing Stream A EventHub Aggregator & Publisher ===");

    // Simulate 3 distinct 10-second windows with different load profiles
    const now = new Date("2026-08-22T19:59:40Z").getTime();

    const mockMessages = [];

    // Window 1 (19:59:40Z) - 300 requests
    for (let i = 0; i < 300; i++) {
        mockMessages.push({
            method: "GET",
            url: "/store/products",
            statusCode: (i < 3) ? 500 : 200,
            duration_ms: Math.floor(100 + Math.random() * 480),
            timestamp: new Date(now + Math.floor(Math.random() * 9500)).toISOString()
        });
    }

    // Window 2 (19:59:50Z) - 340 requests
    for (let i = 0; i < 340; i++) {
        mockMessages.push({
            method: "GET",
            url: "/store/products",
            statusCode: (i < 5) ? 500 : 200,
            duration_ms: Math.floor(120 + Math.random() * 580),
            timestamp: new Date(now + 10000 + Math.floor(Math.random() * 9500)).toISOString()
        });
    }

    // Window 3 (20:00:00Z) - 480 requests (Traffic Spike)
    for (let i = 0; i < 480; i++) {
        mockMessages.push({
            method: "POST",
            url: "/store/carts",
            statusCode: (i < 26) ? 503 : 200,
            duration_ms: Math.floor(250 + Math.random() * 1850),
            timestamp: new Date(now + 20000 + Math.floor(Math.random() * 9500)).toISOString()
        });
    }

    const mockContext = {
        log: (...args) => console.log(...args),
        bindings: {}
    };
    mockContext.log.error = (...args) => console.error(...args);

    const result = await streamAFunc(mockContext, mockMessages);

    console.log("\n=== Resulting Output Windows ===");
    console.log(JSON.stringify(result, null, 2));

    console.log("\n=== Published to context.bindings.outputEventHubMessages ===");
    console.log(mockContext.bindings.outputEventHubMessages ? "Verified Output Binding Set" : "Missing Binding");
}

runTest().catch(console.error);
