#!/usr/bin/python
import os, sys
import numpy
import string
from pylab import *
from matplotlib.ticker import MultipleLocator

from astropy.cosmology import WMAP7
import astropy.units as u
from astropy.constants import G

import datetime as dt

#Calculate and plot the luminsoity/size for the catalog

x1=0.0
x2=10  

# G= 1.3273e26 * u.cm**3 / u.s / u.solMass # cm^3/s/M_*
cm2Mpc=3.086e24 # cm/Mpc

fig_width_pt = 246.0  # Get this from LaTeX using \showthe\columnwidth
inches_per_pt = 1.0/72.27               # Convert pt to inch
golden_mean = (sqrt(5)-1.0)/2.0         # Aesthetic ratio
fig_width = fig_width_pt*inches_per_pt  # width in inches
fig_width = 8.00
fig_height = 8.00
fig_size =  [fig_width,fig_height]
params = {'backend': 'ps',
          'axes.labelsize': 15,
	  'text.fontsize': 15,
	  'legend.fontsize': 15,
	  'xtick.labelsize': 15,
	  'ytick.labelsize': 15,
	  'text.usetex': False,
	  'figure.figsize': fig_size}
rcParams.update(params)



def keyplot( err, xc, yc, sym, mec, mfc):
    plot([xc, xc-0.30], [yc, yc], ls=sym, color=mec, marker='', lw=1.5)
    text(xc-0.35, yc-0.05*yc, err, fontsize='x-large', weight='heavy')

def makeplot(yc, ytext, logy=False, dy=0.8):
    xminorLocator=MultipleLocator(0.2)
    ymsminorLocator=MultipleLocator(0.02)
    yaminorLocator=MultipleLocator(0.1)
     
    axes([0.15, yc, 0.80, dy])

    #set the axis
    msax=gca()
    #msax.xaxis.set_minor_locator(xminorLocator)
    #msax.yaxis.set_minor_locator(ymsminorLocator)
    if logy: msax.set_yscale('log')
    ylabel(ytext,fontsize='x-large')

    if yc > 0.15: msax.set_xticklabels([])

def plotdata(ax, name,norm=False, alpha=1.0, color='#000000', marker='o', iy=18 ):
    lines = open('telescopes.data').readlines()
    l = lines[0].split()
    years = np.array([int(x) for x in l[1:iy]])

    for l in lines[1:]:
        l = l.split()
        if l[0] == name:
           npapers = np.array([float(x) for x in l[1:iy]])
           ntel = float(l[iy+1])
           y = int(l[iy+2])
           c = float(l[iy+3])
           y = years-y
           mask = (y<9) * (y>=0) * (npapers > 0)
           if norm and c<0:  return
           if  not norm: 
              ax.plot(y[mask], npapers[mask]/ntel, marker=marker, alpha=alpha, color=color, label=name)
           else:
               ax.plot(y[mask], npapers[mask]/ntel/c, marker=marker, alpha=alpha, color=color)

           return

def plot_projection(ax, name='SALT-11', iy=18):
    lines = open('telescopes.data').readlines()
    l = lines[0].split()
    years = np.array([int(x) for x in l[1:iy]])

    for l in lines[1:]:
        l = l.split()
        if l[0] == name:
           npapers = np.array([float(x) for x in l[1:iy]])
           print npapers
           ntel = float(l[iy+1])
           y = int(l[iy+2])
           c = float(l[iy+3])
           print name, ntel, y, c
           print npapers
           y = years-y
           print y
           mask = (y<9) * (y>=0) * (npapers > 0)

    m = dt.datetime.now().timetuple().tm_yday
    n1 = npapers[-2]
    n2 = npapers[-1]
    print 'Test ', m, y[-2],y[-1], [n1, n2*365/m]
    ax.plot([y[-2],y[-1]], [n1, n2*365/m], ls='--',alpha=1.0, color='#FF0000', marker='o', mfc='#FFFFFF', mec='#FF0000')


def plot_telpapers():
   ax = axes([0.15, 0.55, 0.8, 0.4])
   xlab='Age [year]'
   ylab='$Papers \ tel^{-1}$'
   #plotdata(ax, 'Keck', norm=False, alpha=0.5, color='#777777', marker='^')
   plotdata(ax, 'VLT', norm=False, alpha=0.5, color='#777777', marker='s')
   plotdata(ax, 'Gemini', norm=False, alpha=0.5, color='#777777', marker='+')
   plotdata(ax, 'Subaru', norm=False, alpha=0.5, color='#777777', marker='p')
   plotdata(ax, 'HET', norm=False, alpha=0.5, color='#777777', marker='d')
   plotdata(ax, 'SALT', norm=False, alpha=1.0, color='#000000', marker='o')
   plot_projection(ax, 'SALT-11')
   plotdata(ax, 'SALT-11', norm=False, alpha=1.0, color='#FF0000', marker='o')

   ax.axis([-0.5, 10, 0, 100])
   ax.set_ylabel(ylab, fontsize='x-large')
   ax.set_xticklabels([])
   ax.legend(loc=2, fontsize='small')

   #cost per year
   ay = axes([0.15, 0.15, 0.8, 0.4])
   #plotdata(ay, 'VLT', norm=True , alpha=0.5, color='#777777', marker='s')
   plotdata(ay, 'Gemini', norm=True , alpha=0.5, color='#777777', marker='+')
   plotdata(ay, 'Subaru', norm=True , alpha=0.5, color='#777777', marker='D')
   #plotdata(ay, 'HET', norm=True , alpha=0.5, color='#777777', marker='d')
   plotdata(ay, 'SALT', norm=True , alpha=1.0, color='#000000', marker='o')
   #ay.plot([2, 3], [11.33, 16.6], ls='--',alpha=1.0, color='#FF0000', marker='o', mfc='#FFFFFF', mec='#FF0000')
   plotdata(ay, 'SALT-11', norm=True , alpha=1.0, color='#FF0000', marker='o')

   ylab='$Papers \ tel^{-1} \ M\$^{-1}$'
   ay.set_xlabel(xlab, fontsize='x-large')
   ay.set_ylabel(ylab, fontsize='x-large')
   ay.axis([-0.5, 10, 0, 13.5])

if __name__=='__main__':
   figure(dpi=72)
   plot_telpapers()
   outfile='fig_telpapers.pdf' 
   savefig(outfile)
   show()
