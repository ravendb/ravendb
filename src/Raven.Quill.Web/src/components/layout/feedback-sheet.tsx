import { useState, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useMutation, useQuery } from "@tanstack/react-query";
import { ThumbsDown, ThumbsUp } from "lucide-react";
import { z } from "zod";
import { toast } from "sonner";
import { api } from "@/api/api";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import {
    SheetClose,
    SheetContent,
    SheetDescription,
    SheetFooter,
    SheetHeader,
    SheetTitle,
    SheetTrigger,
} from "@/components/shadcn/ui/sheet";
import { GuardedSheet } from "@/components/form/unsaved-changes/guarded-overlays";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { FormInput } from "@/components/form/form-input";
import { FormTextarea } from "@/components/form/form-textarea";
import { FormToggleGroup, type FormToggleGroupOption } from "@/components/form/form-toggle-group";
import { withNestedSubmit } from "@/lib/form-utils";
import { getSupportUrl } from "@/lib/help-links";

const IMPRESSION_OPTIONS: FormToggleGroupOption[] = [
    { value: "positive", label: <ThumbsUp />, ariaLabel: "Positive" },
    { value: "negative", label: <ThumbsDown />, ariaLabel: "Negative" },
];

type FeedbackSheetProps = {
    trigger?: ReactNode;
    open?: boolean;
    onOpenChange?: (isOpen: boolean) => void;
};

export function FeedbackSheet({ trigger, open, onOpenChange }: FeedbackSheetProps) {
    const [internalIsOpen, setInternalIsOpen] = useState(false);
    const isOpen = open ?? internalIsOpen;
    const setIsOpen = onOpenChange ?? setInternalIsOpen;

    return (
        <GuardedSheet open={isOpen} onOpenChange={setIsOpen}>
            {trigger && <SheetTrigger asChild>{trigger}</SheetTrigger>}
            <SheetContent className="w-full gap-0 sm:max-w-md data-[side=right]:sm:max-w-md">
                <SheetHeader className="border-b">
                    <SheetTitle>Feedback</SheetTitle>
                    <SheetDescription>
                        Tell us how Quill is working for you. Product and version details are added automatically.
                    </SheetDescription>
                </SheetHeader>
                <FeedbackForm onSent={() => setIsOpen(false)} />
            </SheetContent>
        </GuardedSheet>
    );
}

const feedbackSchema = z.object({
    name: z.string().trim().min(1, "Enter your name"),
    email: z.string().trim().pipe(z.email("Enter a valid email address")),
    impression: z.enum(["positive", "negative"]).nullable(),
    message: z.string().trim().min(1, "Type a message"),
});

type FeedbackFormData = z.infer<typeof feedbackSchema>;

// Don't include pathname because it can contain sensitive information
const STUDIO_VIEW = "Quill";

function FeedbackForm({ onSent }: { onSent: () => void }) {
    const form = useForm<FeedbackFormData>({
        mode: "onChange",
        resolver: zodResolver(feedbackSchema),
        defaultValues: {
            name: "",
            email: "",
            impression: null,
            message: "",
        },
    });

    // Shares the query key with the license and dashboard views, so this is
    // usually a cache hit rather than an extra request.
    const licenseQuery = useQuery(api.queries.settings.license());

    const unsavedChanges = useFormUnsavedChanges(form);

    const submitMutation = useMutation({
        mutationFn: (values: FeedbackFormData) =>
            api.services.settings.feedback({ ...values, studioView: STUDIO_VIEW }),
        onSuccess: () => {
            toast.success("Feedback sent. Thank you.");
            form.reset();
            unsavedChanges.markSaved();
            onSent();
        },
        onError: (error) => {
            toast.error(error instanceof Error ? error.message : "Couldn't send feedback. Please try again.");
        },
    });

    return (
        <form
            className="flex min-h-0 flex-1 flex-col"
            onSubmit={withNestedSubmit(form.handleSubmit((values) => submitMutation.mutate(values)))}
        >
            <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
                <FormInput control={form.control} name="name" label="Name" placeholder="Your name" />
                <FormInput
                    control={form.control}
                    name="email"
                    type="email"
                    label="Email"
                    placeholder="you@company.com"
                    description="We'll use this address if we need to follow up."
                />
                <FormToggleGroup
                    control={form.control}
                    name="impression"
                    label="Impression"
                    options={IMPRESSION_OPTIONS}
                    description="How has Quill worked for you?"
                />
                <FormTextarea
                    control={form.control}
                    name="message"
                    label="Your message"
                    placeholder="Type your message here."
                    textareaClassName="min-h-40 resize-none"
                />
                <p className="text-muted-foreground">
                    Blocked by something broken? Get faster help from{" "}
                    <a
                        href={getSupportUrl(licenseQuery.data?.response.id)}
                        target="_blank"
                        rel="noreferrer"
                        className="underline underline-offset-2 hover:text-foreground"
                    >
                        Support
                    </a>
                    .
                </p>
            </div>

            <SheetFooter className="border-t">
                <Button type="submit" className="w-full" disabled={submitMutation.isPending}>
                    {submitMutation.isPending && <Spinner />}
                    Send feedback
                </Button>
                <SheetClose asChild>
                    <Button type="button" variant="outline" className="w-full" disabled={submitMutation.isPending}>
                        Close
                    </Button>
                </SheetClose>
            </SheetFooter>
        </form>
    );
}
