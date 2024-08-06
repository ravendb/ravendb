import { useEffect } from "react";

interface UseStudioSearchMouseEventsProps {
    inputRef: React.RefObject<HTMLInputElement>;
    studioSearchBackdropId: string;
    setIsDropdownOpen: React.Dispatch<React.SetStateAction<boolean>>;
}

export function useStudioSearchMouseEvents(props: UseStudioSearchMouseEventsProps) {
    const { inputRef, studioSearchBackdropId, setIsDropdownOpen } = props;

    // Handle opening the dropdown by input focus
    useEffect(() => {
        inputRef.current.onfocus = () => {
            setIsDropdownOpen(true);
        };
    }, [inputRef, setIsDropdownOpen]);

    // Handle closing the dropdown by clicking on the backdrop
    useEffect(() => {
        const handleMouseDown = (e: any) => {
            if ((e.target as Element).id === studioSearchBackdropId) {
                setIsDropdownOpen(false);
            }
        };

        document.addEventListener("mousedown", handleMouseDown);
        return () => {
            document.removeEventListener("mousedown", handleMouseDown);
        };
    }, [inputRef, studioSearchBackdropId, setIsDropdownOpen]);
}
