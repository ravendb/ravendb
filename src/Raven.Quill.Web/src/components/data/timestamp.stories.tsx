import type { ReactNode } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { Alert, AlertDescription } from "@/components/shadcn/ui/alert";
import { Badge } from "@/components/shadcn/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { Heading, Text } from "@/components/typography";
import { MS_IN } from "@/lib/time";
import { Timestamp } from "./timestamp";

const minutesAgo = (minutes: number) => new Date(Date.now() - minutes * MS_IN.minute).toISOString();

const SAMPLE_CHANNELS = [
    { name: "Support widget", type: "Web widget", createdAt: minutesAgo(0.5) },
    { name: "Sales chat", type: "Web widget", createdAt: minutesAgo(19) },
    { name: "#help-desk", type: "Slack", createdAt: minutesAgo(3 * 60) },
    { name: "#general", type: "Discord", createdAt: minutesAgo(17 * 60) },
    { name: "Order updates", type: "Telegram", createdAt: minutesAgo(4 * 24 * 60) },
    { name: "Returns bot", type: "Slack", createdAt: minutesAgo(3 * 7 * 24 * 60) },
    { name: "Legacy embed", type: "Web widget", createdAt: minutesAgo(8 * 30 * 24 * 60) },
    { name: "Pilot widget", type: "Web widget", createdAt: minutesAgo(2 * 365 * 24 * 60) },
];

// Mirrors the channel cards' stat box, the tightest place a timestamp has to fit.
function StatBox({ label, value }: { label: string; value: ReactNode }) {
    return (
        <div className="rounded-md border bg-muted/30 px-2.5 py-1.5">
            <div className="text-[11px] text-muted-foreground">{label}</div>
            <Text as="div" variant="label" className="tabular-nums">
                {value}
            </Text>
        </div>
    );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
    return (
        <section className="space-y-2">
            <Heading as="h3" variant="label">
                {title}
            </Heading>
            {children}
        </section>
    );
}

function TimestampGallery() {
    const recent = SAMPLE_CHANNELS[3].createdAt;

    return (
        <div className="space-y-8 p-6">
            <div className="space-y-1">
                <Heading as="h2" variant="section">
                    Timestamp
                </Heading>
                <Text variant="muted">
                    The full variant shows the date and time outright. The short variant drops the time of day and
                    carries it in a tooltip instead.
                </Text>
            </div>

            <Section title={'dateVariant="full" in a table'}>
                <Table>
                    <TableHeader>
                        <TableRow>
                            <TableHead>Channel</TableHead>
                            <TableHead>Type</TableHead>
                            <TableHead>Created</TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {SAMPLE_CHANNELS.map((channel) => (
                            <TableRow key={channel.name}>
                                <TableCell className="font-medium">{channel.name}</TableCell>
                                <TableCell>
                                    <Text as="span" variant="muted">
                                        {channel.type}
                                    </Text>
                                </TableCell>
                                <TableCell>
                                    <Timestamp value={channel.createdAt} />
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </Section>

            <Section title={'dateVariant="short" on cards'}>
                <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                    {SAMPLE_CHANNELS.slice(0, 3).map((channel) => (
                        <Card key={channel.name}>
                            <CardHeader>
                                <CardTitle>{channel.name}</CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="grid grid-cols-2 gap-2">
                                    <StatBox label="Active links" value="3" />
                                    <StatBox
                                        label="Added"
                                        value={
                                            <Timestamp
                                                value={channel.createdAt}
                                                dateVariant="short"
                                                textVariant="inherit"
                                            />
                                        }
                                    />
                                </div>
                            </CardContent>
                        </Card>
                    ))}
                </div>
            </Section>

            <Section title={'textVariant="inherit" inside a sentence'}>
                <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="success">Token valid</Badge>
                    <Text as="span" variant="caption">
                        Last message <Timestamp value={recent} textVariant="inherit" />
                    </Text>
                </div>
                <Alert variant="destructive">
                    <AlertDescription>
                        A delivery failed signature verification at <Timestamp value={recent} textVariant="inherit" /> —
                        the signing secret configured here likely differs from the Slack app&apos;s.
                    </AlertDescription>
                </Alert>
            </Section>

            <Section title="Missing value">
                <Timestamp value={null} />
            </Section>
        </div>
    );
}

const meta = {
    title: "Components/Timestamp",
    component: TimestampGallery,
    parameters: { layout: "fullscreen" },
} satisfies Meta<typeof TimestampGallery>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Gallery: Story = {};
