import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { z } from "zod";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { FormInput } from "@/components/form/form-input";
import { withNestedSubmit } from "@/lib/form-utils";

const phoneNumberSchema = z.object({
    phoneNumber: z
        .string()
        .trim()
        .refine((value) => value.replace(/\D/g, "").length >= 6, "Enter the full number, including the country code"),
});

type PhoneNumberFormData = z.infer<typeof phoneNumberSchema>;

export function WhatsAppPhoneNumberForm({
    onSubmit,
    onCancel,
    isSubmitting,
}: {
    onSubmit: (phoneNumber: string) => void;
    onCancel: () => void;
    isSubmitting: boolean;
}) {
    const form = useForm<PhoneNumberFormData>({
        mode: "onChange",
        resolver: zodResolver(phoneNumberSchema),
        defaultValues: { phoneNumber: "" },
    });

    return (
        <form
            className="flex max-w-sm flex-col gap-3"
            onSubmit={withNestedSubmit(form.handleSubmit((values) => onSubmit(values.phoneNumber)))}
        >
            <FormInput
                control={form.control}
                name="phoneNumber"
                label="Phone number"
                placeholder="+48 601 234 567"
                description="The number of the phone you are linking, with its country code."
            />
            <div className="flex gap-2">
                <Button type="submit" disabled={isSubmitting}>
                    {isSubmitting && <Spinner />}
                    Get pairing code
                </Button>
                <Button type="button" variant="outline" onClick={onCancel} disabled={isSubmitting}>
                    Cancel
                </Button>
            </div>
        </form>
    );
}
