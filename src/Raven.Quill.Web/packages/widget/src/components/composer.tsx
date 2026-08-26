import { useLayoutEffect, useRef, useState } from "react";
import { ArrowUpIcon, StopIcon } from "@/components/icons";

const MAX_TEXTAREA_HEIGHT_PX = 160;

// A textarea has no intrinsic auto-grow, so its height is re-measured from the content.
function fitToContent(textarea: HTMLTextAreaElement | null) {
    if (textarea === null) return;
    textarea.style.height = "auto";
    textarea.style.height = `${Math.min(textarea.scrollHeight, MAX_TEXTAREA_HEIGHT_PX)}px`;
}

type ComposerProps = {
    placeholder: string;
    isStreaming: boolean;
    isDisabled: boolean;
    onSubmit: (prompt: string) => void;
    onStop: () => void;
};

export function Composer({ placeholder, isStreaming, isDisabled, onSubmit, onStop }: ComposerProps) {
    const [value, setValue] = useState("");
    const textareaRef = useRef<HTMLTextAreaElement>(null);
    const formRef = useRef<HTMLFormElement>(null);

    useLayoutEffect(() => fitToContent(textareaRef.current), [value]);

    // How many lines the same text wraps into depends on the width, so a resize (a phone rotating, the
    // theme editor switching preview widths) has to re-measure too - otherwise the box keeps the height
    // the old width earned and the text spills out of it. The form is observed rather than the textarea
    // because the height set above is not part of the form's width, so the measurement cannot feed itself.
    useLayoutEffect(() => {
        const form = formRef.current;
        if (form === null) return;

        let lastWidth = form.clientWidth;
        const observer = new ResizeObserver(() => {
            if (form.clientWidth === lastWidth) return;
            lastWidth = form.clientWidth;
            fitToContent(textareaRef.current);
        });

        observer.observe(form);
        return () => observer.disconnect();
    }, []);

    const canSend = value.trim().length > 0 && isStreaming === false && isDisabled === false;

    const submit = () => {
        if (canSend === false) return;
        onSubmit(value.trim());
        setValue("");
    };

    return (
        <form
            ref={formRef}
            className="border-rq-border shrink-0 border-t px-[var(--rq-pad-x)] py-[var(--rq-pad-y)]"
            onSubmit={(event) => {
                event.preventDefault();
                submit();
            }}
        >
            <div className="rq-composer-box border-rq-border bg-rq-surface focus-within:border-rq-accent flex items-end gap-2 border py-1.5 ps-3.5 pe-1.5">
                <textarea
                    ref={textareaRef}
                    rows={1}
                    value={value}
                    disabled={isDisabled}
                    placeholder={placeholder}
                    aria-label={placeholder}
                    onChange={(event) => setValue(event.currentTarget.value)}
                    onKeyDown={(event) => {
                        // `isComposing`: an IME fires Enter to confirm a candidate, not to send.
                        if (event.key !== "Enter" || event.shiftKey || event.nativeEvent.isComposing) return;
                        event.preventDefault();
                        submit();
                    }}
                    className="placeholder:text-rq-muted my-1.5 max-h-40 flex-1 resize-none bg-transparent text-sm leading-normal focus:outline-none disabled:opacity-60"
                />
                {isStreaming ? (
                    <button
                        type="button"
                        onClick={onStop}
                        aria-label="Stop generating"
                        className="bg-rq-accent text-rq-accent-fg hover:bg-rq-accent-hover focus-visible:ring-rq-accent focus-visible:ring-offset-rq-bg rounded-rq-pill flex size-9 shrink-0 items-center justify-center transition-colors focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:outline-none"
                    >
                        <StopIcon className="size-4" />
                    </button>
                ) : (
                    <button
                        type="submit"
                        disabled={canSend === false}
                        aria-label="Send message"
                        className="bg-rq-accent text-rq-accent-fg hover:bg-rq-accent-hover focus-visible:ring-rq-accent focus-visible:ring-offset-rq-bg rounded-rq-pill flex size-9 shrink-0 items-center justify-center transition-colors focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:outline-none disabled:opacity-40"
                    >
                        <ArrowUpIcon className="size-4" />
                    </button>
                )}
            </div>
        </form>
    );
}
