import { useState, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useMutation } from "@tanstack/react-query";
import { z } from "zod";
import { toast } from "sonner";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import {
    Sheet,
    SheetClose,
    SheetContent,
    SheetDescription,
    SheetFooter,
    SheetHeader,
    SheetTitle,
    SheetTrigger,
} from "@/components/shadcn/ui/sheet";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { FormTextarea } from "@/components/form/form-textarea";
import { withNestedSubmit } from "@/lib/form-utils";

const SUBJECT_OPTIONS: FormSelectOption<string>[] = [
    { value: "licensing", label: "Licensing" },
    { value: "billing", label: "Billing" },
    { value: "technical", label: "Technical support" },
    { value: "sales", label: "Sales" },
    { value: "other", label: "Other" },
];

export function ContactSheet({ trigger }: { trigger: ReactNode }) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <Sheet open={isOpen} onOpenChange={setIsOpen}>
            <SheetTrigger asChild>{trigger}</SheetTrigger>
            <SheetContent className="w-full gap-0 sm:max-w-md data-[side=right]:sm:max-w-md">
                <SheetHeader className="border-b">
                    <SheetTitle>Contact</SheetTitle>
                    <SheetDescription>
                        We&apos;ll auto-attach your app name, license token, and email so sales can find your account.
                    </SheetDescription>
                </SheetHeader>
                <ContactForm onSent={() => setIsOpen(false)} />
            </SheetContent>
        </Sheet>
    );
}

const contactSchema = z.object({
    subject: z.string().min(1, "Pick a subject"),
    replyToEmail: z.union([z.literal(""), z.email("Enter a valid email address")]),
    message: z.string().trim().min(1, "Type a message"),
});

type ContactFormData = z.infer<typeof contactSchema>;

function ContactForm({ onSent }: { onSent: () => void }) {
    const form = useForm<ContactFormData>({
        mode: "onChange",
        resolver: zodResolver(contactSchema),
        defaultValues: {
            subject: "licensing",
            replyToEmail: "",
            message: "",
        },
    });

    const submitMutation = useMutation({
        // No contact endpoint exists yet — this prepares the UI so it can be pointed
        // at the real service once it lands. Simulate a round-trip for now.
        mutationFn: async (values: ContactFormData) => {
            await new Promise((resolve) => setTimeout(resolve, 600));
            return values;
        },
        onSuccess: () => {
            toast.success("Thanks — our team will get back to you shortly.");
            onSent();
        },
    });

    return (
        <form
            className="flex min-h-0 flex-1 flex-col"
            onSubmit={withNestedSubmit(form.handleSubmit((values) => submitMutation.mutate(values)))}
        >
            <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
                <FormSelect
                    control={form.control}
                    name="subject"
                    label="Subject"
                    options={SUBJECT_OPTIONS}
                    placeholder="Pick a subject"
                />
                <FormInput
                    control={form.control}
                    name="replyToEmail"
                    type="email"
                    label="Reply-to email"
                    placeholder="you@company.com"
                    description="We'll route the reply here if it differs from your account."
                />
                <FormTextarea
                    control={form.control}
                    name="message"
                    label="Your message"
                    placeholder="Type your message here."
                    textareaClassName="min-h-40 resize-none"
                />
            </div>

            <SheetFooter className="border-t">
                <Button type="submit" className="w-full" disabled={submitMutation.isPending}>
                    {submitMutation.isPending && <Spinner />}
                    Submit
                </Button>
                <SheetClose asChild>
                    <Button type="button" variant="outline" className="w-full">
                        Close
                    </Button>
                </SheetClose>
            </SheetFooter>
        </form>
    );
}
