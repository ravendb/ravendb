import { zodResolver } from "@hookform/resolvers/zod";
import { useState, type ReactNode } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";
import { FormInput } from "@/components/form/form-input";
import { Button } from "@/components/shadcn/ui/button";
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/shadcn/ui/dialog";

type ChannelPreviewDialogProps = {
    widgetId: string;
    displayName: string;
    /** The agent's declared chat-scoped parameter names — when present, the
     * preview collects a value for each before loading the widget. */
    parameterNames: string[];
    trigger: ReactNode;
};

const previewParametersSchema = z.object({
    parameters: z.array(
        z.object({
            name: z.string(),
            value: z.string().trim().min(1, "Required"),
        }),
    ),
});

type PreviewParametersFormValues = z.infer<typeof previewParametersSchema>;

// Frames the live /embed/{widgetId} page — the exact page customers iframe on
// their sites. The relative URL is same-origin in production (the appliance
// serves both) and proxied to the backend in dev (vite.config.ts).
// Agent parameters travel on the embed URL's query string, exactly as an
// embedding site would pass them; the embed page forwards them on every chat turn.
export function ChannelPreviewDialog({ widgetId, displayName, parameterNames, trigger }: ChannelPreviewDialogProps) {
    // Applied separately from the form state so typing doesn't reload the
    // iframe on every keystroke; re-applying reloads it with the new values.
    const [appliedSearch, setAppliedSearch] = useState<string | null>(parameterNames.length === 0 ? "" : null);

    const { control, handleSubmit } = useForm<PreviewParametersFormValues>({
        resolver: zodResolver(previewParametersSchema),
        defaultValues: { parameters: parameterNames.map((name) => ({ name, value: "" })) },
    });

    const applyParameters = handleSubmit((values) => {
        const query = new URLSearchParams(values.parameters.map(({ name, value }) => [name, value.trim()]));
        setAppliedSearch(`?${query.toString()}`);
    });

    const embedUrl = appliedSearch === null ? null : `/embed/${widgetId}${appliedSearch}`;

    return (
        <Dialog>
            <DialogTrigger asChild>{trigger}</DialogTrigger>
            <DialogContent className="sm:max-w-md">
                <DialogHeader>
                    <DialogTitle>Widget preview</DialogTitle>
                    <DialogDescription>
                        How “{displayName}” looks when embedded on a site.
                        {embedUrl && (
                            <>
                                {" "}
                                <a href={embedUrl} target="_blank" rel="noreferrer">
                                    Open in a new tab
                                </a>
                            </>
                        )}
                    </DialogDescription>
                </DialogHeader>
                {parameterNames.length > 0 && (
                    <form className="grid gap-3" onSubmit={applyParameters}>
                        {parameterNames.map((name, index) => (
                            <FormInput key={name} control={control} name={`parameters.${index}.value`} label={name} />
                        ))}
                        <Button type="submit" size="sm" className="justify-self-end">
                            {embedUrl ? "Apply parameters" : "Load preview"}
                        </Button>
                    </form>
                )}
                {embedUrl ? (
                    /* The embed page styles itself light-only, so the backdrop stays white in dark mode too. */
                    <iframe
                        src={embedUrl}
                        title={`${displayName} widget preview`}
                        className="h-[min(60vh,600px)] w-full rounded-lg border bg-white"
                    />
                ) : (
                    <div className="flex h-[min(60vh,600px)] w-full items-center justify-center rounded-lg border px-6 text-center text-sm text-muted-foreground">
                        The agent requires these parameters — fill them in to load the preview.
                    </div>
                )}
            </DialogContent>
        </Dialog>
    );
}
