import { PagePanel } from "@/components/data/page-panel";
import { Heading, Text } from "@/components/typography";

type SimpleInfoPageProps = {
    title: string;
    description: string;
};

export function SimpleInfoPage({ description, title }: SimpleInfoPageProps) {
    return (
        <PagePanel>
            <div className="max-w-2xl space-y-2">
                <Heading variant="subsection">{title}</Heading>
                <Text variant="muted">{description}</Text>
            </div>
        </PagePanel>
    );
}
