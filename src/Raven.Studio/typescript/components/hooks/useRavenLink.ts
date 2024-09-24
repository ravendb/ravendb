import { todo } from "common/developmentHelper";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useAppSelector } from "components/store";

type UseRavenLinkProps = {
    hash: string;
} & (
    | {
          isDocs: false;
          version?: never;
          lang?: never;
      }
    | {
          isDocs?: true;
          version?: string;
          lang?: "Csharp" | "Java" | "Python" | "NodeJs";
      }
);

export function useRavenLink(props: UseRavenLinkProps): string {
    return useGetRavenLink()(props);
}

export function useGetRavenLink(): (props: UseRavenLinkProps) => string {
    const clientVersion = useAppSelector(clusterSelectors.clientVersion);

    return (props: UseRavenLinkProps): string => {
        const { hash, lang, isDocs = true, version = clientVersion } = props;

        let link = `https://ravendb.net/l/${hash}`;

        if (isDocs) {
            link += `/${version}`;

            if (lang) {
                link += `/${lang}`;
            }
        }

        todo("Feature", "ANY", "Add utm source");

        return link;
    };
}
