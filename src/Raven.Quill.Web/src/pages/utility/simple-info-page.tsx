import { PagePanel } from "@/components/data/page-panel";

type SimpleInfoPageProps = {
    title: string;
    description: string;
};

export function SimpleInfoPage({ description, title }: SimpleInfoPageProps) {
    return (
        <PagePanel>
            <div className="max-w-2xl space-y-2">
                <h2 className="text-base font-semibold">{title}</h2>
                <p className="text-sm text-muted-foreground">{description}</p>
            </div>
        </PagePanel>
    );
}
