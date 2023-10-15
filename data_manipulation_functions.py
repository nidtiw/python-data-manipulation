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
def css_queue_manager(csv_file_path,logger):
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
Scenario: Many devices with telemetry data in one giant csv files.
Input: csv file path
process_function: function you want to perform on each file
max_obj : Max number of objects for one process
"""
def viewnet_queue_manager(csv_file_path, max_obj,logger):
    mp.log_to_stderr(logging.DEBUG)
    group_list = []
    queue_list = []
    file_list = [i for i in glob.glob(csv_file_path)]
    sorted_list = sorted(file_list, key=lambda x: x.split('.')[1][:10])
    logger.info(sorted_list)
    for file_num, file_name in enumerate(sorted_list[:-1]):
        file_list2 = []
        file_list2.append(sorted_list[file_num])
        file_list2.append(sorted_list[file_num+1])
        serve_df = pd.concat((pd.read_csv(f, compression='gzip', header = 0, low_memory=False) for f in file_list2))
        #TODO: Check the place if this line "group_list = []"
        ig_groups = serve_df.groupby(by='lite_id')
        for k1, v1 in ig_groups:
            v1['created_at'] = pd.to_datetime(v1['created_at'])
            v1.sort_values(by='created_at', inplace=True)
            st, et = v1['created_at'].iloc[0] + pd.Timedelta(hours=site_timezone_lookup[site_id]), v1['created_at'].iloc[0] + pd.Timedelta(hours=24+site_timezone_lookup[site_id])
            v1 = v1[(v1['created_at'] > st) & (v1['created_at'] < et)]
            if (k1 is not None) or (k1!=''):
                queue_list.append(v1)
                if len(queue_list) == max_obj:
                    group_list.append(pd.concat(queue_list))
                    queue_list.clear()
        if len(queue_list) > 0:
            group_list.append(pd.concat(queue_list))
            queue_list.clear()
    p = mp.Pool(processes = mp.cpu_count()-1)
    try:
        results = [p.apply_async(viewnet_process, args=(df,site_id, logger)) for df in group_list]
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
Check how many discrepencies in data reporting / missing values there are in telemetry data in the form of a time series.
"""
def write_freq_discrepancy(df):
    num_entries = df.shape[0]
    from_date = np.array(pd.to_datetime(df['date'].iloc[0:num_entries-2]))
    to_date = np.array(pd.to_datetime(df['date'].iloc[1:(num_entries - 1)]) + pd.Timedelta('1m'))
    write_freq = np.floor((np.subtract(to_date, from_date)/np.timedelta64(1, 's'))/60)
    writeError_count = (write_freq > 4).sum()
    writeError_idx = np.where(write_freq > 4)[0]
    return writeError_idx, writeError_count