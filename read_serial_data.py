# Function of this script: read data from serial connection and plot the same on a tkinter app that updates as serial daat updates.

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import tkinter as tk
import numpy as np
import serial as sr
import time

#-----------Global variables
# data = np.array([[],[],[]])
data_sp_voltage = np.array([])
data_act_voltage = np.array([])
data_current = np.array([])
cond = False

#------------plot data
def plot_data():
    global cond
    global data_sp_voltage
    global data_act_voltage
    global data_current
    if cond == True:
        a1 = s.readline()   # reads in UTF
        print(a1)
        a = a1.decode().split(',') # converts to string
        print('tis is a: {}'.format(a))
        # subplot 1
        try:
            if(len(data_sp_voltage) < 50):
                data_sp_voltage = np.append(data_sp_voltage, float(a[4])*1000)   # convert ASCII to floating point number
                print(data_sp_voltage)
            else:
                data_sp_voltage[0:49] = data_sp_voltage[1:50]
                data_sp_voltage[49] = float(a[4])*1000
                print(data_sp_voltage)
            lines1.set_xdata(np.arange(0, len(data_sp_voltage)))
            lines1.set_ydata(data_sp_voltage)

            # subplot 2
            if(len(data_act_voltage) < 50):
                data_act_voltage = np.append(data_act_voltage, float(a[5])*1000)   # convert ASCII to floating point number
                print(data_act_voltage)
            else:
                data_act_voltage[0:49] = data_act_voltage[1:50]
                data_act_voltage[49] = float(a[5])*1000
                print(data_act_voltage)
            lines2.set_xdata(np.arange(0, len(data_act_voltage)))
            lines2.set_ydata(data_act_voltage)

            # subplot 3
            if(len(data_current) < 50):
                data_current = np.append(data_current, float(a[6])*1000)   # convert ASCII to floating point number
                print(data_current)
            else:
                data_current[0:49] = data_current[1:50]
                data_current[49] = float(a[6])*1000
                print(data_current)
            lines3.set_xdata(np.arange(0, len(data_current)))
            lines3.set_ydata(data_current)
        except Exception as e:
            print(e)

        canvas.draw()
    root.after(1, plot_data)

def plot_start():
    global cond
    cond = True
    s.reset_input_buffer()

def plot_stop():
    global cond
    cond = False

#------
root = tk.Tk()
root.title('Real Time Plot')
root.configure(background = 'light blue')
root.geometry("700x500")

fig = Figure();
ax = fig.add_subplot(131)

ax.set_title('Serial Data');
ax.set_xlabel('Sample')
ax.set_ylabel('Required Voltage')
ax.set_xlim(0, 50)
ax.set_ylim(-2500, 3000)
lines1 = ax.plot([], [])[0]

ax = fig.add_subplot(132)

ax.set_title('Serial Data');
ax.set_xlabel('Sample')
ax.set_ylabel('Real Voltage')
ax.set_xlim(0, 50)
ax.set_ylim(-2500, 3000)
lines2 = ax.plot([], [])[0]

ax = fig.add_subplot(133)

ax.set_title('Serial Data');
ax.set_xlabel('Sample')
ax.set_ylabel('Current')
ax.set_xlim(0, 50)
ax.set_ylim(-1050, 1050)
lines3 = ax.plot([], [])[0]

canvas = FigureCanvasTkAgg(fig, master=root)
canvas.get_tk_widget().place(x=10, y=10, width=800, height = 400)
canvas.draw()

#------------Create buttons
root.update();
start = tk.Button(root, text="Start", font=('calibri',12),command = lambda: plot_start())
start.place(x = 100, y= 450)

root.update();
stop = tk.Button(root, text = "Stop", font = ('calibri', 12), command = lambda: plot_stop())
stop.place(x = start.winfo_x()+start.winfo_reqwidth() + 20, y=450)

#start serial port
s = sr.Serial('COM4', 9600)

root.after(1, plot_data)
root.mainloop()