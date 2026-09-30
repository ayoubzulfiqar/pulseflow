import { createElement } from "react";
import { EmbedConfig, AuditRecord } from "./types";

export const PulseFlowEmbed = ({
  config,
  onReady,
}: {
  config: EmbedConfig;
  onReady?: () => void;
}) => {
  const { PulseFlowEmbedImpl } = usePulseFlow(config);
  return createElement(PulseFlowEmbedImpl, { config, onReady });
};

function usePulseFlow(config: EmbedConfig) {
  const { default: PulseFlowEmbedImpl } = {} as { default: React.ComponentType<any> };
  return { PulseFlowEmbedImpl: PulseFlowEmbedImpl };
}
