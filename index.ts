#!/usr/bin/env node

import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { estypes } from "@elastic/elasticsearch";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";

// Simplified configuration schema with only URL and API key
const ConfigSchema = z.object({
  url: z
    .string()
    .trim()
    .min(1, "Elasticsearch URL cannot be empty")
    .url("Invalid Elasticsearch URL format")
    .describe("Elasticsearch server URL"),

  apiKey: z
    .string()
    .trim()
    .describe("API key for Elasticsearch authentication"),
});

type ElasticsearchConfig = z.infer<typeof ConfigSchema>;

export async function createElasticsearchMcpServer(
  config: ElasticsearchConfig
) {
  const validatedConfig = ConfigSchema.parse(config);
  const { url } = validatedConfig;

  const server = new McpServer({
    name: "elasticsearch-mcp-server",
    version: "0.1.0",
  });

  // Tool 1: List indices
  server.tool(
    "list_indices",
    "List all available Elasticsearch indices",
    {},
    async () => {
      try {
        const response = await fetch(`${url}/_cat/indices?format=json`);
        if (!response.ok) {
          throw new Error(`Failed to fetch indices: ${response.statusText}`);
        }
        const responseData = await response.json() as any;

        const indicesInfo = responseData.map((index) => ({
          index: index.index,
          health: index.health,
          status: index.status,
          docsCount: index.docsCount,
        }));

        return {
          content: [
            {
              type: "text" as const,
              text: `Found ${indicesInfo.length} indices`,
            },
            {
              type: "text" as const,
              text: JSON.stringify(indicesInfo, null, 2),
            },
          ],
        };
      } catch (error) {
        console.error(
          `Failed to list indices: ${
            error instanceof Error ? error.message : String(error)
          }`
        );
        return {
          content: [
            {
              type: "text" as const,
              text: `Error: ${
                error instanceof Error ? error.message : String(error)
              }`,
            },
          ],
        };
      }
    }
  );

  server.tool(
    "list_aliases",
    "List all available Elasticsearch aliases",
    {},
    async () => {
      try {
        const response = await fetch(`${url}/_cat/aliases?format=json`);
        if (!response.ok) {
          throw new Error(`Failed to fetch aliases: ${response.statusText}`);
        }
        const responseData = await response.json() as any;

        const indicesInfo = responseData.map((alias) => ({
          alias: alias.alias,
          index: alias.index,
        }));

        return {
          content: [
            {
              type: "text" as const,
              text: `Found ${indicesInfo.length} aliases`,
            },
            {
              type: "text" as const,
              text: JSON.stringify(indicesInfo, null, 2),
            },
          ],
        };
      } catch (error) {
        console.error(
          `Failed to list aliases: ${
            error instanceof Error ? error.message : String(error)
          }`
        );
        return {
          content: [
            {
              type: "text" as const,
              text: `Error: ${
                error instanceof Error ? error.message : String(error)
              }`,
            },
          ],
        };
      }
    }
  );

  // Tool 2: Get mappings for an index
  server.tool(
    "get_mappings_of_index",
    "Get field mappings for a specific Elasticsearch index",
    {
      index: z
        .string()
        .trim()
        .min(1, "Index name is required")
        .describe("Name of the Elasticsearch index to get mappings for"),
    },
    async ({ index }) => {
      try {
        const response = await fetch(`${url}/${index}/_mapping`);
        if (!response.ok) {
          throw new Error(`Failed to fetch indices: ${response.statusText}`);
        }
        const responseData = await response.json() as any;

        return {
          content: [
            {
              type: "text" as const,
              text: `Mappings for index: ${index}`,
            },
            {
              type: "text" as const,
              text: `Mappings for index ${index}: ${JSON.stringify(
                responseData[index]?.mappings || {},
                null,
                2
              )}`,
            },
          ],
        };
      } catch (error) {
        console.error(
          `Failed to get mappings: ${
            error instanceof Error ? error.message : String(error)
          }`
        );
        return {
          content: [
            {
              type: "text" as const,
              text: `Error: ${
                error instanceof Error ? error.message : String(error)
              }`,
            },
          ],
        };
      }
    }
  );

  server.tool(
    "get_mappings_of_alias",
    "Get field mappings for a specific Elasticsearch alias",
    {
      alias: z
        .string()
        .trim()
        .min(1, "Alias name is required")
        .describe("Name of the Elasticsearch alias to get mappings for"),
    },
    async ({ alias }) => {
      try {
        const response = await fetch(`${url}/${alias}/_mapping`);
        if (!response.ok) {
          throw new Error(`Failed to fetch indices: ${response.statusText}`);
        }
        const responseData = await response.json() as any;

        return {
          content: [
            {
              type: "text" as const,
              text: `Mappings for alias: ${alias}`,
            },
            {
              type: "text" as const,
              text: `Indexes for alias ${alias}: ${JSON.stringify(
                Object.keys(responseData) || [],
                null,
                2
              )}`,
            },
            {
              type: "text" as const,
              text: `Mappings for alias ${alias}: ${JSON.stringify(
                responseData || {},
                null,
                2
              )}`,
            },
          ],
        };
      } catch (error) {
        console.error(
          `Failed to get mappings: ${
            error instanceof Error ? error.message : String(error)
          }`
        );
        return {
          content: [
            {
              type: "text" as const,
              text: `Error: ${
                error instanceof Error ? error.message : String(error)
              }`,
            },
          ],
        };
      }
    }
  );

  // Tool 3: Search an index with simplified parameters
  server.tool(
    "search",
    "Perform an Elasticsearch search with the provided query DSL.",
    {
      index_or_alias: z
        .string()
        .trim()
        .min(1, "Index or alias name is required")
        .describe("Name of the Elasticsearch index or alias to search"),

      queryBody: z
        .record(z.any())
        .refine(
          (val) => {
            try {
              JSON.parse(JSON.stringify(val));
              return true;
            } catch (e) {
              return false;
            }
          },
          {
            message: "queryBody must be a valid Elasticsearch query DSL object",
          }
        )
        .describe(
          "Complete Elasticsearch query DSL object that can include query, size, from, sort, etc."
        ),
    },
    async ({ index_or_alias, queryBody }) => {
      try {
        const searchRequest: estypes.SearchRequest = {
          ...queryBody,
        };
        const body = JSON.stringify(searchRequest);

        const response = await fetch(`${url}/${index_or_alias}/_search`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body,
        });
        if (!response.ok) {
          throw new Error(`Failed to fetch indices: ${response.statusText} response: ${await response.text()}`);
        }
        const result = await response.json() as estypes.SearchResponse;

        // Extract the 'from' parameter from queryBody, defaulting to 0 if not provided
        const from = queryBody.from || 0;

        const contentFragments = result.hits.hits.map((hit) => {
          const highlightedFields = hit.highlight || {};
          const sourceData = hit._source || {};

          let content = "";

          for (const [field, highlights] of Object.entries(highlightedFields)) {
            const highlightedField = highlights as any;
            if (highlightedField && highlightedField.length > 0) {
              content += `${field} (highlighted): ${highlightedField.join(
                " ... "
              )}\n`;
            }
          }

          for (const [field, value] of Object.entries(sourceData)) {
            if (!(field in highlightedFields)) {
              content += `${field}: ${JSON.stringify(value)}\n`;
            }
          }

          return {
            type: "text" as const,
            text: content.trim(),
          };
        });

        const metadataFragment = {
          type: "text" as const,
          text: `Total results: ${
            typeof result.hits.total === "number"
              ? result.hits.total
              : result.hits.total?.value || 0
          }, showing ${result.hits.hits.length} from position ${from}`,
        };

        return {
          content: [metadataFragment, ...contentFragments],
        };
      } catch (error) {
        console.error(
          `Search failed: ${
            error instanceof Error ? error.message : String(error)
          }`
        );
        return {
          content: [
            {
              type: "text" as const,
              text: `Error: ${
                error instanceof Error ? error.message : String(error)
              }`,
            },
          ],
        };
      }
    }
  );

  return server;
}

const config: ElasticsearchConfig = {
  url: process.env.ES_URL || "",
  apiKey: process.env.ES_API_KEY || "",
};

async function main() {
  const transport = new StdioServerTransport();
  const server = await createElasticsearchMcpServer(config);

  await server.connect(transport);

  process.on("SIGINT", async () => {
    await server.close();
    process.exit(0);
  });
}

main().catch((error) => {
  console.error(
    "Server error:",
    error instanceof Error ? error.message : String(error)
  );
  process.exit(1);
});