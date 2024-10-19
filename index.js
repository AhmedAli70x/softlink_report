const sdk = require("node-appwrite");
require('dotenv').config(); // Load environment variables

const client = new sdk.Client();

client
    .setEndpoint(process.env.CMS_ENDPOINT)
    .setProject(process.env.CMS_PROJECT)
    .setKey(process.env.CMS_API_KEY);

const databases = new sdk.Databases(client);

// Function to summarize data from the CMS_COLLECTION_SHORTURLS and CMS_COLLECTION_URLTRACKING tables
async function summarizeClientData() {
    try {
        const limit = 100;
        let offset = 0;
        const clientSummary = {};

        // Fetch data from ShortURLs table
        let response = await databases.listDocuments(
            process.env.CMS_DATABASE,
            process.env.CMS_COLLECTION_SHORTURLS,
            [
                sdk.Query.limit(limit),
                sdk.Query.offset(offset)
            ]
        );
        let totalDocuments = response.total;

        // Process ShortURLs data to initialize clientSummary
        response.documents.forEach(doc => {
            const { client, shortCode } = doc;
            if (!clientSummary[client]) {
                clientSummary[client] = {
                    shortCodes: []
                };
            }
            clientSummary[client].shortCodes.push(shortCode);
        });

        // Fetch remaining documents from ShortURLs
        while (offset + limit < totalDocuments) {
            offset += limit;
            response = await databases.listDocuments(
                process.env.CMS_DATABASE,
                process.env.CMS_COLLECTION_SHORTURLS,
                [
                    sdk.Query.limit(limit),
                    sdk.Query.offset(offset)
                ]
            );
            response.documents.forEach(doc => {
                const { client, shortCode } = doc;
                if (!clientSummary[client]) {
                    clientSummary[client] = {
                        shortCodes: []
                    };
                }
                clientSummary[client].shortCodes.push(shortCode);
            });
        }

        // Return the client summary as JSON
        return clientSummary;

    } catch (error) {
        console.error("Failed to fetch documents:", error);
    }
}

async function queryShortCodesForMonth(year, month, summaryResult) {
    
    console.log(summaryResult.shortCodes)
    for (let client in summaryResult) {
        // console.log(summaryResult[client].shortCodes);

        if (summaryResult[client].shortCodes && summaryResult[client].shortCodes.length > 0) {
            // console.log(`${client} has shortCodes:`, summaryResult[client].shortCodes);
            const shortCodesList = summaryResult[client].shortCodes;
            const startDate = new Date(year, month - 1, 1).toISOString(); // Start of the month
            const endDate = new Date(year, month, 1).toISOString(); // Start of the next month
            const shortCodesData = {
                countries:{},
                devices:{}
            }; // To hold the final summarized results

            shortCodesList.forEach( async function(shortLink) {
                console.log(shortLink);

                const startDate = new Date(year, month - 1, 1).toISOString(); // Start of the month
                const endDate = new Date(year, month, 1).toISOString(); // Start of the next month
                const results = {}; // To hold the final summarized result

                const limit = 100; // Limit for pagination
                let offset = 0; // Offset for pagination

                // Fetch data from URLTRACKING collection for the specific shortCode and month
                let response = await databases.listDocuments(
                    process.env.CMS_DATABASE,
                    process.env.CMS_COLLECTION_URLTRACKING,
                    [
                        sdk.Query.equal('shortCode', shortLink), // Filter by shortCode
                        sdk.Query.greaterThanEqual('$createdAt', startDate), // Filter by start of the month
                        sdk.Query.lessThan('$createdAt', endDate), // Filter by start of the next month
                        sdk.Query.limit(limit),
                        sdk.Query.offset(offset),
                        sdk.Query.select(["shortCode", "country", "deviceType"])
                       
                    ]
                );

                linkTotalVisits =  response.total

                console.log(response.documents)

            });


        } else {
            // console.log(`${client} has no shortCodes.`);
        }
    }
    return;
    try {
        const startDate = new Date(year, month - 1, 1).toISOString(); // Start of the month
        const endDate = new Date(year, month, 1).toISOString(); // Start of the next month
        const results = {}; // To hold the final summarized results

        // Iterate over each client in the summaryResult dictionary
        for (const [client, data] of Object.entries(summaryResult)) {
            results[client] = { shortCodes: {} };

            // Query for each shortCode associated with the client
            for (const shortCode of data.shortCodes) {
                const limit = 100; // Limit for pagination
                let offset = 0; // Offset for pagination
                const shortCodeResults = []; // To hold documents for the current shortCode

                // Fetch data from URLTRACKING collection for the specific shortCode and month
                let response = await databases.listDocuments(
                    process.env.CMS_DATABASE,
                    process.env.CMS_COLLECTION_URLTRACKING,
                    [
                        sdk.Query.equal('shortCode', shortCode), // Filter by shortCode
                        sdk.Query.greaterThanEqual('$createdAt', startDate), // Filter by start of the month
                        sdk.Query.lessThan('$createdAt', endDate), // Filter by start of the next month
                        sdk.Query.limit(limit),
                        sdk.Query.offset(offset),
                        sdk.Query.select(["shortCode", "country", "deviceType"])
                       
                    ]
                );

                let totalDocuments = response.total;
                console.log(response.documents)
                // Collect all documents matching the shortCode and date range
                while (offset + limit < totalDocuments) {
                    shortCodeResults.push(...response.documents); // Add current batch of results

                    // Increment offset and fetch more documents
                    offset += limit;
                    response = await databases.listDocuments(
                        process.env.CMS_DATABASE,
                        process.env.CMS_COLLECTION_URLTRACKING,
                        [
                            sdk.Query.equal('shortCode', shortCode),
                            sdk.Query.greaterThanEqual('$createdAt', startDate),
                            sdk.Query.lessThan('$createdAt', endDate),
                            sdk.Query.limit(limit),
                            sdk.Query.offset(offset)
                        ]
                    );
                }

                // Add the results to the final output under the respective client and shortCode
                results[client].shortCodesData[shortCode] = shortCodeResults;
            }
        }

        // Return or process the final summarized results as needed
        return results;

    } catch (error) {
        console.error("Failed to query documents for the month:", error);
    }
}

// Example of how to call the function to summarize client data
summarizeClientData().then(summaryResult => {
    queryShortCodesForMonth(2024, 8, summaryResult).then(monthlyResult => {
        console.log("Summarized Data for August 2024:", JSON.stringify(monthlyResult, null, 2));
    }).catch(error => {
        console.error("Error fetching summarized data:", error);
    });
}).catch(error => {
    console.error("Error fetching summary:", error);
});
