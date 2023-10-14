# Functions I use repeatedly in scross projects involving slicing, indexing, deriving metrics from telemetry data from devices. These are tailored for controllers but can typically be used for any telemetry data set up in csv files.

"""
Class for picking out specific indexes/ calculate summary metrics from telemetry data from devices.
metric: column name/ field. The particular paramter of the telemetry data you are working on.
step_df : user defined, used if you are splitting your time series into multiple phases.
Example usage: Calculate the difference between t_start and t_end --> 'elapsed_time' : (Calculate_Metrics(phase, 'date').get_difference().total_seconds() / 60)
N.B. : metrics can be calculated above separately and stored in a dictionary efficiently.
"""
class Calculate_Metrics:
    def __init__(self, step_df, metric):
        self.step_df = step_df
        self.metric = metric
        if len(step_df) > 1:
            self.step_series = step_df.filter(regex='^{}'.format(metric)).squeeze()
        else:
            self.step_series = step_df.filter(regex='^{}'.format(metric))

    def get_max(self):
        return self.step_series.max()

    def get_unique(self):
        return self.step_series.unique()[0] if len(self.step_series) > 1 else self.step_series.iloc[0,0]

    def get_first_index(self):
        return self.step_series.iloc[0] if len(self.step_series) > 1 else self.step_series.iloc[0,0]
    
    def get_second_last_index(self):
        return self.step_series.iloc[-2] if len(self.step_series) > 1 else self.step_series.iloc[0,0]
    
    def get_last_index(self):
        return self.step_series.iloc[-1] if len(self.step_series) > 1 else self.step_series.iloc[0,0]

    def get_difference(self):
        return self.step_series.iloc[-1] - self.step_series.iloc[0] if len(self.step_series) > 1 else timedelta(0)

    def get_absolute_max(self):
        return self.step_series.iloc[self.step_series.abs().idxmax()] if len(self.step_series) > 1 else self.step_series.iloc[0,0]
    
    def get_median(self):
        return self.step_series.median() if len(self.step_series) > 1 else self.step_series.iloc[0,0]

    def get_last_nonZero(self):
        step_series = self.step_series
        # print(step_series)
        if (len(step_series) > 1 and (step_series == 0).all()) or (len(step_series) == 1 and step_series.iloc[0,:].all() == 0):
            return 0.0
        else:
            return self.step_series[step_series.loc[lambda step_series: step_series.ne(0)].index[-1]] if len(step_series) > 1 else step_series.iloc[0,0]

"""
Use pandas groupby to group based on a column ('column_name') value, without bunching all the rows containing a value in one group, but maintaining their index in the dataframe.
Output: Get all groups and the keys (main keys and column keys) to help reference the groups for later operations.
"""
def groupby_state(each_df):
    filtered = each_df.filter(regex = '^column_name').squeeze()
    each_df_groups =  each_df.groupby((filtered != filtered.shift()).cumsum(), as_index=False)
    group_keys = list(each_df_groups.groups.keys())
    column_keys = [v.filter(regex='^column_name').squeeze().unique()[0] if len(v) > 1 else v.filter(regex='^column_name').squeeze() for k, v in each_df_groups]
    return each_df_groups, group_keys, column_keys

# Two functions applying asynchronous multiprocessing in different format, depending on the input data structure.
"""
Scenario: Many devices with telemetry data split across multiple csv files. For ex: 4000 devices, 5 days of time series data spread across 40 csv files.
Input: csv files path
process_function: function you want to perform on each file
"""
def css_queue_manager(site_id,  BASE_PATH,logger):
    mp.log_to_stderr(logging.DEBUG)
    file_list = []
    for put_file in glob.glob(csv_file_path):
        logger.info('file added: {}'.format(put_file))
        file_list.append(put_file)
    p = mp.Pool(processes = mp.cpu_count()-1)
    try:
        results = [p.apply_async(process_function, args) for file in file_list]
    except Exception as e:
        logger.error('Error in multiprocessing. See error message below')
        logger.error(e)
    p.close()
    p.join()
    try:
        output = [res.get() for res in results]
        flat_list = [x for xs in output for x in xs]
        final_output = pd.DataFrame(flat_list, columns=cols)
        return final_output
    except Exception as e:
        logger.error('Error in compiling result dataframe')
        logger.error(e)


"""
Scenario: Many devices with telemetry data split across multiple csv files. For ex: 4000 devices, 5 days of time series data spread across 40 csv files.
Input: csv files path
process_function: function you want to perform on each file
"""