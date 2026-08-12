import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { telegramParameterSourceLabel } from "@/pages/apps/channels/telegram-parameter-sources";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

export function TelegramChannelBindings({ channel }: { channel: ChannelSummaryResponse }) {
    const bindings = Object.entries(channel.telegram?.parameterBindings ?? {});

    return (
        <SectionCard
            title="Parameter bindings"
            description="How each agent parameter is filled for conversations on this channel."
        >
            <SectionTable
                headers={["Parameter", "Source", "Value"]}
                isEmpty={bindings.length === 0}
                emptyMessage="The agent declares no parameters, so this channel binds nothing."
            >
                {bindings.map(([name, binding]) => (
                    <TableRow key={name} className="hover:bg-transparent">
                        <TableCell className="font-mono text-xs">{name}</TableCell>
                        <TableCell className="text-sm">{telegramParameterSourceLabel(binding.source)}</TableCell>
                        <TableCell className="text-sm">
                            {binding.source === "Constant" ? (
                                binding.value
                            ) : (
                                <span className="text-muted-foreground">Bound from the message sender</span>
                            )}
                        </TableCell>
                    </TableRow>
                ))}
            </SectionTable>
        </SectionCard>
    );
}
