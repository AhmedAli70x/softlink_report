const sdk = require("node-appwrite");
require('dotenv').config(); // Load environment variables

const client = new sdk.Client();

client
    .setEndpoint(process.env.CMS_ENDPOINT)
    .setProject(process.env.CMS_PROJECT)
    .setKey(process.env.CMS_API_KEY);

const databases = new sdk.Databases(client);
const clientSummary = {};

// Function to summarize data from the CMS_COLLECTION_SHORTURLS and CMS_COLLECTION_URLTRACKING tables
async function summarizeClientData() {
    try {
        const limit = 100;
        let offset = 0;

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
                    shortCodes: [],
                    shortCodesTracking: {}
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
                        shortCodes: [],
                        shortCodesTracking: {}
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

function countVisits(visits) {
    const result = {};
  
    visits.forEach(({ shortCode, IP }) => {
      // Initialize the shortCode in the result map if not already present
      if (!result[shortCode]) {
        result[shortCode] = {
          totalVisits: 0,
          uniqueVisits: new Set() // Use a Set to track unique IPs
        };
      }
  
      // Increment the total visit count
      result[shortCode].totalVisits += 1;
  
      // Add the IP to the Set of unique visits
      result[shortCode].uniqueVisits.add(IP);
    });
  
    // Convert the uniqueVisits Set to count the number of unique IPs
    for (const shortCode in result) {
      result[shortCode].uniqueVisits = result[shortCode].uniqueVisits.size;
    }
  
    return result;
  }

async function queryShortCodesForMonth(year, month, summaryResult) {
    
    // console.log(summaryResult.shortCodes)
    for (let client in summaryResult) {
        // console.log(summaryResult[client].shortCodes);

        if (summaryResult[client].shortCodes && summaryResult[client].shortCodes.length > 0) {
            // console.log(`${client} has shortCodes:`, summaryResult[client].shortCodes);
            const shortCodesList = summaryResult[client].shortCodes;
            const startDate = new Date(year, month - 1, 1).toISOString(); // Start of the month
            const endDate = new Date(year, month, 1).toISOString(); // Start of the next month
           // To hold the final summarized results
            shortCodesList.forEach( async function(shortLink) {
                const limit = 100; // Limit for pagination
                let offset = 0; // Offset for pagination
                let shortCodesData = {
                    countries:{},
                    devices:{},
                    totalVisits: 0,
                    totalVisitsUnique: new Set() ,
                    locations: new Set() 
                };
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
                        sdk.Query.select(["shortCode", "country", "deviceType","IP", "IPLocationGoogle",  "$createdAt"])
                       
                    ]
                );
                let linkTotalVisits =  response.total              

            
                if ( linkTotalVisits > 0){

                    // Using reduce to count visits per country
                    let visitCountByCountry = response.documents.reduce((acc, visit) => {
                        // If the country is already in the accumulator, increment its count
                        if (acc[visit.country]) {
                        acc[visit.country]++;
                        } else {
                        // If the country is not in the accumulator, initialize it with 1
                        acc[visit.country] = 1;
                        }
                        return acc;
                    }, {});

                    // Using reduce to count visits per device
                    let visitCountByDevice = response.documents.reduce((acc, visit) => {
                        // If the country is already in the accumulator, increment its count
                        if (acc[visit.deviceType]) {
                        acc[visit.deviceType]++;
                        } else {
                        // If the country is not in the accumulator, initialize it with 1
                        acc[visit.deviceType] = 1;
                        }
                        return acc;
                    }, {});
                    
                    response.documents.forEach((data) =>{
                        
                    shortCodesData.totalVisitsUnique.add(data.IP)
                    shortCodesData.locations.add(data.IPLocationGoogle)
                         
                    })
                    
                    shortCodesData.countries= {...visitCountByCountry}
                    shortCodesData.devices= {...visitCountByDevice}
                    shortCodesData.totalVisits= response.total 
                    shortCodesData.totalVisitsUnique= shortCodesData.totalVisitsUnique.size
                    shortCodesData.locations= [...shortCodesData.locations]

                    console.log(shortCodesData)
                    console.log(shortLink)
                    
                    clientSummary[client].shortCodesTracking[shortLink]=JSON.stringify(shortCodesData)
                    
                    console.log(clientSummary)
                    
                    
                }

                else{
                    // clientSummary[client].shortCodesTracking[shortLink]=0

                }
  

            });
            // console.log(results)
            
            // console.log(clientSummary);
            // console.log(JSON.stringify(summaryResult, null, 2));
            
        } else {
            // console.log(`${client} has no shortCodes.`);
        }
    }
    return ;
    
}

// Example of how to call the function to summarize client data
summarizeClientData().then(summaryResult => {
    queryShortCodesForMonth(2024, 8, summaryResult).then(monthlyResult => {
        // console.log("Summarized Data for August 2024:", JSON.stringify(monthlyResult, null, 2));
    }).catch(error => {
        // console.error("Error fetching summarized data:", error);
    });
}).catch(error => {
    // console.error("Error fetching summary:", error);
});
