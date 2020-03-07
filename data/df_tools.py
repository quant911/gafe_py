# DataFrame and other Pandas related tools
# below is less than ideal, do we know why?

import pandas as pd


def waterfall_combine(combine_field, *args):
    # type: (str, List[pd.DataFrame]) -> pd.DataFrame
    df = pd.DataFrame()
    for one_df in args:
        if not one_df.empty:
            if one_df.index.name != combine_field:
                one_df = one_df.set_index(combine_field, drop=False)

            if not df.empty:
                one_df = one_df.drop(df.index, errors='ignore')

            df = pd.concat([df, one_df])

    df.reset_index(inplace=True, drop=True)

    if len(df) == 0:
        df[combine_field] = None # minimal requirement: the waterfall col exists

    #todo: do we impose condition on Index of output DataFrame

    return df


def waterfall_combine_dict(combine_field, df_dic, waterfall_sources):
    # type: (str, Dict[str, pd.DataFrame], List[str]) -> pd.DataFrame
    return waterfall_combine(combine_field, [df_dic[s] for s in waterfall_sources])
