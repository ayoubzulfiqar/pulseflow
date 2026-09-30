import { createElement, useState, useEffect } from "react";
import { PulseFlowEmbedClient } from "./client";
import type { EmbedConfig, AuditRecord, Destination } from "./types";

type Props = {
  config: EmbedConfig;
  onReady?: () => void;
};

export function PulseFlowEmbed({ config, onReady }: Props) {
  const [client] = useState(() => new PulseFlowEmbedClient(config));
  const [deliveries, setDeliveries] = useState<AuditRecord[]>([]);
  const [destinations, setDestinations] = useState<Destination[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<"deliveries" | "destinations">("deliveries");

  useEffect(() => {
    void client.authenticate();
    onReady?.();
  }, [client, onReady]);

  const refreshDeliveries = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await client.getDeliveries(50);
      setDeliveries((res.deliveries || []) as AuditRecord[]);
    } catch (err: any) {
      setError(err.message);
    }
    setLoading(false);
  };

  const refreshDestinations = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await client.getDestinations();
      setDestinations((res.destinations || []) as Destination[]);
    } catch (err: any) {
      setError(err.message);
    }
    setLoading(false);
  };

  const retryDelivery = async (destId: string) => {
    try {
      await client.retryDelivery(destId);
      void refreshDeliveries();
    } catch (err: any) {
      setError(err.message);
    }
  };

  // Auto-refresh on tab focus.
  useEffect(() => {
    if (activeTab === "deliveries") void refreshDeliveries();
    if (activeTab === "destinations") void refreshDestinations();
  }, [activeTab]);

  const theme = config.theme || {};

  return createElement(
    "div",
    {
      className: "pulseflow-embed",
      style: {
        fontFamily: theme.font || "-apple-system, BlinkMacSystemFont, sans-serif",
        borderRadius: theme.borderRadius || "8px",
        "--pf-primary": theme.primaryColor || "#3b82f6",
      } as any,
    },
    createElement(
      "div",
      { className: "pf-header" },
      !config.hideBranding &&
        createElement("span", { className: "pf-logo" }, "PulseFlow"),
      createElement(
        "div",
        { className: "pf-tabs" },
        createElement(
          "button",
          {
            className: activeTab === "deliveries" ? "active" : "",
            onClick: () => setActiveTab("deliveries"),
          },
          "Delivery Logs"
        ),
        createElement(
          "button",
          {
            className: activeTab === "destinations" ? "active" : "",
            onClick: () => setActiveTab("destinations"),
          },
          "Destinations"
        )
      )
    ),
    createElement(
      "div",
      { className: "pf-content" },
      loading && createElement("div", { className: "pf-loading" }, "Loading..."),
      error && createElement("div", { className: "pf-error" }, error),
      activeTab === "deliveries" &&
        createElement("table", { className: "pf-table" },
          createElement("thead", null,
            createElement("tr", null,
              createElement("th", null, "Timestamp"),
              createElement("th", null, "Event ID"),
              createElement("th", null, "Status"),
              createElement("th", null, "Reason"),
              createElement("th", null, "Action")
            )
          ),
          createElement("tbody", null,
            deliveries.map((d) =>
              createElement("tr", { key: d.id },
                createElement("td", null, new Date(d.timestamp).toLocaleString()),
                createElement("td", null, d.event_id?.slice(0, 8)),
                createElement("td", null, d.status),
                createElement("td", null, d.reason),
                d.status === "failed" && !config.readOnly &&
                  createElement("td", null,
                    createElement("button", {
                      onClick: () => retryDelivery(d.destination_id),
                    }, "Retry"))
              )
            )
          )
        ),
      activeTab === "destinations" &&
        createElement("table", { className: "pf-table" },
          createElement("thead", null,
            createElement("tr", null,
              createElement("th", null, "URL"),
              createElement("th", null, "Pattern"),
              createElement("th", null, "Status"),
              createElement("th", null, "Concurrency")
            )
          ),
          createElement("tbody", null,
            destinations.map((d) =>
              createElement("tr", { key: d.id },
                createElement("td", null, d.url),
                createElement("td", null, d.event_pattern),
                createElement("td", null, d.status),
                createElement("td", null, d.concurrency_limit)
              )
            )
          )
        )
    )
  );
}

// Styles (injected via CSS class — users should include their own CSS or use the CDN bundle)
export const PulseFlowEmbedStyles = `
.pulseflow-embed { max-width: 100%; font-size: 14px; color: #333; }
.pf-header { display: flex; justify-content: space-between; align-items: center; padding: 12px 16px; border-bottom: 1px solid #e5e7eb; background: #f9fafb; }
.pf-logo { font-weight: 600; color: var(--pf-primary, #3b82f6); }
.pf-tabs { display: flex; gap: 4px; padding: 8px 16px; border-bottom: 1px solid #e5e7eb; }
.pf-tabs button { padding: 8px 16px; border: none; background: none; cursor: pointer; border-radius: 4px 4px 0 0; }
.pf-tabs button.active { background: var(--pf-primary, #3b82f6); color: white; }
.pf-content { padding: 16px; }
.pf-loading { text-align: center; padding: 40px; color: #6b7280; }
.pf-error { color: #ef4444; padding: 8px 16px; }
.pf-table { width: 100%; border-collapse: collapse; }
.pf-table th, .pf-table td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #e5e7eb; }
.pf-table th { font-weight: 600; color: #374151; font-size: 12px; text-transform: uppercase; }
.pf-table tr:hover { background: #f9fafb; }
`;
