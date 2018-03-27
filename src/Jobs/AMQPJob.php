<?php

namespace Forumhouse\LaravelAmqp\Jobs;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Jobs\Job;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Exception;

/**
 * Job class for AMPQ jobs
 *
 * @package Forumhouse\LaravelAmqp\Jobs
 */
class AMQPJob extends Job implements \Illuminate\Contracts\Queue\Job
{
    /**
     * @var string
     */
    protected $queue;

    /**
     * @var AMQPMessage
     */
    protected $amqpMessage;
    /**
     * @var AMQPChannel
     */
    private $channel;

    /**
     * @param Container $container
     * @param string    $queue
     * @param           $channel
     * @param string    $amqpMessage
     */
    public function __construct($container, $queue, $channel, $amqpMessage)
    {
        $this->container = $container;
        $this->queue = $queue;
        $this->amqpMessage = $amqpMessage;
        $this->channel = $channel;
    }
    
    /**
     * Get the raw body string for the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        return $this->amqpMessage->body;
    }

    /**
     * Release the job back into the queue.
     *
     * @param  int $delay
     *
     * @return void
	 *
	 * * @throws Exception
     */
    public function release($delay = 0)
    {
		$this->delete();

		$body = $this->amqpMessage->body;
		$body = json_decode($body, true);
		/** @var IAMQPJobBase $job */
		$job = unserialize($body['data']['command']);
		if ($job instanceof IAMQPJobBase != true)
			throw new Exception("JOB IS NOT AN AMQP JOB");

		$job->setAttempts($job->getAttempts() + 1);

		/** @var QueueContract $queue */
		$queue = $this->container['queue']->connection();
		if ($delay > 0) {
			$queue->later($delay, $job, null, $this->getQueue());
		} else {
			$queue->push($job, null, $this->getQueue());
		}
    }

    /**
     * Delete the job from the queue.
     *
     * @return void
     */
    public function delete()
    {
        parent::delete();
        $this->channel->basic_ack($this->amqpMessage->delivery_info['delivery_tag']);
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
	 *
	 * @throws Exception
     */
    public function attempts()
    {
		$body = json_decode($this->amqpMessage->body, true);
		/** @var IAMQPJobBase $job */
		$job = unserialize($body['data']['command']);
		if ($job instanceof IAMQPJobBase != true)
			throw new Exception("JOB IS NOT AN AMQP JOB");

		return $job->getAttempts();
    }

    /**
     * Get queue name
     *
     * @return string
     */
    public function getQueue()
    {
        return $this->queue;
    }

    /**
     * Get the job identifier.
     *
     * @return string
     * @throws \OutOfBoundsException
     */
    public function getJobId()
    {
        return $this->amqpMessage->get('message_id');
    }
}
