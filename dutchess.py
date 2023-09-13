import dutchie
import asyncio

from pandas import DataFrame, Series
import pandas as pd
import numpy as np

def dispensary_df(ds):
    def records():
        for d in ds:
            yield dict(
                d_id=d.id,
                d_name=d.name,
                d_url_name=d.cName,
                d_url=d.embedBackUrl,
            )
    return DataFrame.from_records(records())

def product_info_df(ps):
    def records():
        for p in ps:
            yield dict(
                p_id=p.id,
                d_id=p.DispensaryID,
                brand=p.brandName,
                name=p.Name,
                url_name=p.cName,
            )
            
    return DataFrame.from_records(records())

def terpene_info(ps, terpene_keep_count=5):
    """DataFrame index by product id (p_id). Columns are `terpene_keep_count` most popular terpenes"""
    def records():
        for p in ps:
            if not p.terpenes:
                continue
            for t in p.terpenes:
                if t.unit != 'PERCENTAGE':
                    continue
                yield dict(
                    p_id = p.id,
                    terpene_name = t.libraryTerpene.name,
                    terpene_percent = t.value,
                )
    # One row for each product, one column for each terpene
    df = (
        DataFrame.from_records(records())
        .set_index('p_id')
        .pivot(columns='terpene_name', values='terpene_percent')
    )
    # Filter out elements where all terpene data is zero or null; likely sketchy data
    df = df[df.sum(axis=1) > 0]
    
    # Find the most commonly documented terpenes (terpenes with most non-null values), while forcing Linalool to always be present.
    terpene_counts = df.notna().sum()
    terpene_counts.loc['Linalool'] = np.inf
    terpene_names = terpene_counts.nlargest(terpene_keep_count).index.values
    df = df[terpene_names]
    return df

def combined_df(ds, ps):
    d_df = dispensary_df(ds)
    p_df = product_info_df(ps)
    linalool_df = terpene_info(ps)[['Linalool']]

    df = (
        p_df
        .merge(d_df, on='d_id')
        .merge(linalool_df, on='p_id')
    )
    
    def url_guess(row):
        return f'https://dutchie.com/dispensary/{row.d_url_name}/product/{row.url_name}'
    
    df = (
        df
        .assign(url_guess=df.apply(url_guess, axis=1))
        .rename(columns=dict(
            Linalool='linalool',
            d_name='dispensary',
            d_url='dispensary_url',
        ))
    )
    return df.sort_values('linalool')[['dispensary', 'dispensary_url', 'brand', 'name', 'url_guess', 'linalool']]
    
async def amain():
    ds, ps = await dutchie.load(25)
    df = combined_df(ds, ps)
    df.to_csv('linalool.csv')
    
def main():
    asyncio.run(amain())

if __name__ == '__main__':
    main()
