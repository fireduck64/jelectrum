Mini Server
-----------

The objective is to create a small and cheap (less than $300) fully functional electrum server.
The hardware listed here is an example.


Requirements
------------

 * At least 4GB of RAM
 * At least 120GB of SSD (250 GB recommended for future expansion)
 * 64-bit CPU (for memory mapped key value stores), ideally a GHz or two and a few cores

Current Hardware Recommendations
--------------------------------

This is the hardware running mashtk6hmnysevfj.onion listed on [Electrum Server Monitor](https://1209k.com/bitcoin-eye/ele.php).
All links are Amazon Affiliate links, so purchasing through the links will help fund this project.

 * [Gigabyte Intel Celeron N2807 Mini PC Barebones GB-BXBT-2807](http://amzn.to/1kIEPB7)
 * [Kingston Technology 4GB 1600MHz DDR3L SODIMM](http://amzn.to/1NwMbkS)
 * [Crucial BX100 250GB SATA 2.5](http://amzn.to/1Mfee4M)

Right now, this totals to about $221 before tax and shipping.

You'll need to install an OS via a USB drive.  You'll need a USB keyboard and something that can display
HDMI to complete the install.  I'd just connect it to ethernet if you can.  I haven't tried the wireless
on this box.  Other than that, this is all the hardware that you will need.

Software Setup
--------------

I recommend the latest Debian.  That is jessie at the time of this writing.

apt-get install ant git screen openjdk-7-jdk


