"""Archive.org catalog search query builder and deep scraper."""

from __future__ import annotations
import logging
from typing import AsyncIterator, Dict, Any, List, Optional, Union

from .client import ArchiveClient, ArchiveError

logger = logging.getLogger("archivist.search")


class QueryBuilder:
    """Lucene query builder for Archive.org catalog searches."""

    def __init__(self):
        self._clauses: List[str] = []

    def raw(self, clause: str) -> "QueryBuilder":
        self._clauses.append(clause)
        return self

    def collection(self, collection_name: str) -> "QueryBuilder":
        self._clauses.append(f"collection:({collection_name})")
        return self

    def mediatype(self, mediatype_name: str) -> "QueryBuilder":
        self._clauses.append(f"mediatype:({mediatype_name})")
        return self

    def creator(self, creator_name: str) -> "QueryBuilder":
        self._clauses.append(f'creator:("{creator_name}")')
        return self

    def title(self, title_text: str) -> "QueryBuilder":
        self._clauses.append(f'title:("{title_text}")')
        return self

    def date_range(self, start_date: str, end_date: str) -> "QueryBuilder":
        self._clauses.append(f"date:[{start_date} TO {end_date}]")
        return self

    def year(self, year: Union[int, str]) -> "QueryBuilder":
        self._clauses.append(f"year:{year}")
        return self

    def subject(self, subject_name: str) -> "QueryBuilder":
        self._clauses.append(f'subject:("{subject_name}")')
        return self

    def license(self, license_url: str) -> "QueryBuilder":
        self._clauses.append(f'licenseurl:("*{license_url}*")')
        return self

    def format(self, file_format: str) -> "QueryBuilder":
        self._clauses.append(f"format:({file_format})")
        return self

    def build(self) -> str:
        if not self._clauses:
            return "*:*"
        return " AND ".join(self._clauses)


class SearchEngine:
    """Catalog discovery engine with paging and continuous cursor scraping."""

    def __init__(self, client: ArchiveClient):
        self.client = client

    async def iterate_search(
        self,
        query: str,
        fields: Optional[List[str]] = None,
        max_results: Optional[int] = 100,
        page_size: int = 50,
        sorts: Optional[List[str]] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        """Asynchronously stream search results page-by-page with duplicate integrity checking."""
        page = 1
        yielded = 0
        seen_identifiers = set()

        while True:
            res = await self.client.search(
                query=query,
                fields=fields,
                rows=page_size,
                page=page,
                sorts=sorts
            )
            docs = res.get("docs", [])
            if not docs:
                break

            for doc in docs:
                ident = doc.get("identifier")
                if ident:
                    if ident in seen_identifiers:
                        raise ArchiveError(
                            f"paging integrity failure: duplicate identifier '{ident}' returned on page {page}. "
                            f"advancedsearch deep paging is not reliable; use scrape_all instead."
                        )
                    seen_identifiers.add(ident)

                yield doc
                yielded += 1
                if max_results and yielded >= max_results:
                    return

            num_found = res.get("numFound", 0)
            if yielded >= num_found or len(docs) < page_size:
                break

            page += 1

    async def scrape_all(
        self,
        query: str,
        fields: Optional[List[str]] = None,
        max_results: Optional[int] = None,
        cap: Optional[int] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        """Deep scrape using Archive.org Scrape API cursor token (guaranteed unique cursor pagination)."""
        limit = cap if cap is not None else max_results
        cursor = None
        yielded = 0
        seen_identifiers = set()

        while True:
            data = await self.client.scrape(query=query, fields=fields, cursor=cursor)
            items = data.get("items", [])
            if not items:
                break

            for item in items:
                ident = item.get("identifier")
                if ident:
                    if ident in seen_identifiers:
                        continue
                    seen_identifiers.add(ident)

                yield item
                yielded += 1
                if limit and yielded >= limit:
                    return

            cursor = data.get("cursor")
            if not cursor:
                break
