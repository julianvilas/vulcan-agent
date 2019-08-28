package main

import (
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/autoscaling/autoscalingiface"
)

type ASGMock struct {
	autoscalingiface.AutoScalingAPI
}

func (m ASGMock) TerminateInstanceInAutoScalingGroup(input *autoscaling.TerminateInstanceInAutoScalingGroupInput) (*autoscaling.TerminateInstanceInAutoScalingGroupOutput, error) {
	return &autoscaling.TerminateInstanceInAutoScalingGroupOutput{}, nil
}
