import numpy as np
from mpi4py import MPI

comm = MPI.COMM_WORLD
message = np.arange(1)
comm.Barrier()
status = MPI.Status()

size = comm.size
rank = comm.rank

if rank == 0:
        message[0] = 48151623
        print 'process ', rank, ' sends ' , message
	comm.Send(message, dest=1)
        print ' ' 
elif rank == 1:
	comm.Recv(message, source=0,status=status)
        print ' ' 
	print 'process ', rank, ' receives ', message

