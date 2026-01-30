import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useAppSelector } from "components/store";

export type UseRavenLinkProps = {
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
    const { hash, lang } = props;

    const clientVersion = useAppSelector(clusterSelectors.clientVersion);

    const isDocs = props.isDocs ?? true;
    const version = props.version ?? clientVersion;

    let link = `https://ravendb.net/l/${hash}`;

    if (isDocs) {
        link += `/${version}`;

        if (lang) {
            link += `/${lang}`;
        }
    }

    // TODO Add utm source

    return link;
}
