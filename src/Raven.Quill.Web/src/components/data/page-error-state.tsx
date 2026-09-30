import type { ReactNode } from "react";
import { Heading, Text } from "@/components/typography";

type PageErrorStateProps = {
    code?: string;
    title: string;
    description: ReactNode;
    children?: ReactNode;
};

export function PageErrorState({ code, title, description, children }: PageErrorStateProps) {
    return (
        <section className="flex min-h-full flex-col items-center justify-center gap-5 px-4 py-16 text-center">
            {code && <p className="text-6xl font-semibold tracking-tight text-muted-foreground/50">{code}</p>}
            <div className="max-w-md space-y-1.5">
                <Heading as="h1" variant="title">
                    {title}
                </Heading>
                <Text as="div" variant="muted">
                    {description}
                </Text>
            </div>
            {children && <div className="flex flex-wrap items-center justify-center gap-2">{children}</div>}
        </section>
    );
}
