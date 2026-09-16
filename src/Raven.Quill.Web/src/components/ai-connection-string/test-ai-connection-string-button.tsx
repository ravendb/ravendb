import { CircleCheck, PlugZap } from "lucide-react";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { cn } from "@/lib/utils";

type TestAiConnectionStringButtonProps = {
    isVerified: boolean;
    isPending: boolean;
    error: string | null;
    disabled?: boolean;
    onTest: () => void | Promise<void>;
};

export function TestAiConnectionStringButton({
    isVerified,
    isPending,
    error,
    disabled,
    onTest,
}: TestAiConnectionStringButtonProps) {
    return (
        <div className="grid gap-3">
            <div className="flex">
                <Button
                    type="button"
                    variant="outline"
                    onClick={() => void onTest()}
                    disabled={disabled || isPending || isVerified}
                    className={cn(isVerified && "border-success/40 text-success disabled:opacity-100")}
                >
                    {isPending ? <Spinner /> : isVerified ? <CircleCheck /> : <PlugZap />}
                    {isVerified ? "Connection verified" : "Test connection"}
                </Button>
            </div>
            {error && (
                <Alert variant="destructive" className="whitespace-pre-wrap">
                    {error}
                </Alert>
            )}
        </div>
    );
}
