export default function EditCdcSinkTaskWarningMessage({ message }: { message: string }) {
    return (
        <div className="text-break" style={{ whiteSpace: "pre-line" }}>
            {message}
        </div>
    );
}
